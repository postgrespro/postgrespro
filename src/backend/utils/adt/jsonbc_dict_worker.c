/*
 * src/backend/utils/adt/jsonbc_dict_worker.c
 *
 *  Created on: 15.11.2016
 *      Author: Nikita Glukhov
 */

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "mb/pg_wchar.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shm_mq.h"
#include "utils/hsearch.h"
#include "utils/jsonbc_dict.h"
#include "utils/memutils.h"

#define JSONBC_MQ_BUF_SIZE 1024

typedef struct JsonbcDictWorker
{
	Oid					dbid;
	bool				started;

	Latch		   		workerLatch;
	Latch			   	backendLatch;

	struct
	{
		JsonbcDictId	dict;
		JsonbcKeyId		nextKeyId;
		shm_mq		   *keymq;
	} request;

	struct
	{
		JsonbcKeyId		id;
		shm_mq		   *errmq;
	} response;

	char				mqbuf[JSONBC_MQ_BUF_SIZE];
} JsonbcDictWorker;

typedef HTAB JsonbcDictWorkers;

static JsonbcDictWorker	   *jsonbcDictWorker;
static JsonbcDictWorkers   *jsonbcDictWorkers;
static bool 				jsonbcDictWorkerShutdownRequested = false;

#define PG_JSONBC_DICT_SHM_MAGIC 0x8a02fb3d

static void
handle_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	jsonbcDictWorkerShutdownRequested = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	if (jsonbcDictWorker)
		SetLatch(&jsonbcDictWorker->workerLatch);

	errno = save_errno;
}

void
JsonbcDictWorkerShmemInit()
{
	HASHCTL hctl;

	memset(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(Oid);
	hctl.entrysize = sizeof(JsonbcDictWorker);
	hctl.hash = oid_hash;

	jsonbcDictWorkers = ShmemInitHash("jsonbc dictionary workers hash", 16, 128,
									  &hctl, HASH_ELEM | HASH_FUNCTION);
}

static char *
jsonbcDictWorkerReceiveString(shm_mq *mq, int *len)
{
	shm_mq_handle	   *mqh;
	char			   *str;
	void			   *data;
	Size				size;

	shm_mq_set_receiver(mq, MyProc);
	mqh = shm_mq_attach(mq, NULL, NULL);

	if (shm_mq_receive(mqh, &size, &data, false) != SHM_MQ_SUCCESS ||
		size != sizeof(*len))
	{
		shm_mq_detach(mq);
		elog(ERROR, "jsonbc: error reading key len from mq");
	}

	memcpy(len, data, size);

	if (shm_mq_receive(mqh, &size, &data, false) != SHM_MQ_SUCCESS ||
		size != *len)
	{
		shm_mq_detach(mq);
		elog(ERROR, "jsonbc: error reading key from mq");
	}

	str = memcpy(palloc(size), data, size);

	shm_mq_detach(mq);

	return str;
}

static void
jsonbcDictWorkerSendString(shm_mq *mq, const char *str, int len)
{
	shm_mq_handle	   *mqh;

	shm_mq_set_sender(mq, MyProc);
	mqh = shm_mq_attach(mq, NULL, NULL);

	if (shm_mq_wait_for_attach(mqh) != SHM_MQ_SUCCESS)
	{
		shm_mq_detach(mq);
		elog(ERROR, "jsonbc: error attaching to key mq");
	}

	if (shm_mq_send(mqh, sizeof(len), &len, false) != SHM_MQ_SUCCESS ||
		shm_mq_send(mqh, len, str, false) != SHM_MQ_SUCCESS)
	{
		shm_mq_detach(mq);
		elog(ERROR, "jsonbc: error writing to key mq");
	}

	shm_mq_detach(mq);
}

void
JsonbcDictWorkerMain(Datum main_arg)
{
	JsonbcDictWorker	   *wrk = (JsonbcDictWorker	*) DatumGetPointer(main_arg);
	MemoryContext			mcxt;

	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "jsonbc dict wrk toplevel");
	mcxt = AllocSetContextCreate(TopMemoryContext,
								 "jsonbc dict wrk",
								 ALLOCSET_DEFAULT_MINSIZE,
								 ALLOCSET_DEFAULT_INITSIZE,
								 ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(mcxt);

	JsonbcDictWorkerShmemInit();

	BackgroundWorkerInitializeConnectionByOid(wrk->dbid, InvalidOid);

	SetClientEncoding(GetDatabaseEncoding());

	OwnLatch(&wrk->workerLatch);
	ResetLatch(&wrk->workerLatch);
	SetLatch(&wrk->backendLatch);

	jsonbcDictWorker = wrk;

	for (;;)
	{
		int		rc = WaitLatch(&wrk->workerLatch,
							   WL_LATCH_SET | WL_POSTMASTER_DEATH, 0,
							   WAIT_EVENT_JSONBC_DICT_WORKER);

		if (rc & WL_POSTMASTER_DEATH)
			exit(1);

		if (jsonbcDictWorkerShutdownRequested)
			break;

		ResetLatch(&wrk->workerLatch);

		PG_TRY();
		{
			JsonbcKeyName	key;

			key.s = jsonbcDictWorkerReceiveString(wrk->request.keymq, &key.len);


			StartTransactionCommand();

			wrk->response.id = jsonbcDictGetIdByNameSeqCached(wrk->request.dict,
															  key);

			if (wrk->response.id == JsonbcInvalidKeyId)
				wrk->response.id =
						jsonbcDictGetIdByNameSlow(wrk->request.dict,
												  key,
												  wrk->request.nextKeyId);

			CommitTransactionCommand();

			SetLatch(&wrk->backendLatch);
		}
		PG_CATCH();
		{
			ErrorData	   *edata;

			MemoryContextSwitchTo(mcxt);
			edata = CopyErrorData();
			FlushErrorState();
			wrk->response.id = JsonbcInvalidKeyId;
			wrk->response.errmq = shm_mq_create(wrk->mqbuf, sizeof(wrk->mqbuf));

			SetLatch(&wrk->backendLatch);

			jsonbcDictWorkerSendString(wrk->response.errmq,
									   edata->message, strlen(edata->message));

			FreeErrorData(edata);
		}
		PG_END_TRY();
	}

	DisownLatch(&wrk->workerLatch);

	proc_exit(0);
}

static JsonbcDictWorker *
jsonbcDictWorkerStart(Oid dbid)
{
	BackgroundWorker		bgworker;
	BackgroundWorkerHandle *bgwhandle;
	JsonbcDictWorker	   *wrk;
	LWLock				   *partitionLock;
	MemoryContext			oldcontext;
	BgwHandleStatus			status;
	pid_t					pid;
	uint32					hashcode;
	int						rc;
	bool					found;

	hashcode = oid_hash(&dbid, sizeof(dbid));
	partitionLock = LockHashPartitionLock(hashcode);
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	wrk = hash_search_with_hash_value(jsonbcDictWorkers, &dbid, hashcode,
									  HASH_ENTER, &found);

	LWLockRelease(partitionLock);


	if (found && wrk->started)
		return wrk;

	if (!found)
	{
		InitSharedLatch(&wrk->workerLatch);
		InitSharedLatch(&wrk->backendLatch);
	}

	elog(DEBUG1, "starting jsonbc dictionary background worker for DB %d", dbid);

	/* We might be running in a short-lived memory context. */
	oldcontext = MemoryContextSwitchTo(TopTransactionContext);

	/* Configure a worker. */
	snprintf(bgworker.bgw_name, BGW_MAXLEN, "jsonbc dictionary worker");
	bgworker.bgw_flags = BGWORKER_SHMEM_ACCESS |
						 BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgworker.bgw_start_time = BgWorkerStart_ConsistentState;
	bgworker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(bgworker.bgw_library_name, "postgres");
	sprintf(bgworker.bgw_function_name, "JsonbcDictWorkerMain");
	bgworker.bgw_main_arg = PointerGetDatum(wrk);
	bgworker.bgw_notify_pid = MyProcPid;

	OwnLatch(&wrk->backendLatch);
	ResetLatch(&wrk->backendLatch);

	PG_TRY();
	{
		if (!RegisterDynamicBackgroundWorker(&bgworker, &bgwhandle))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("could not register jsonbc dictionary worker"),
					 errhint("Consider increasing the configuration parameter "
							 "\"max_worker_processes\"")));

		status = WaitForBackgroundWorkerStartup(bgwhandle, &pid);

		if (status == BGWH_STOPPED)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("could not start background process"),
				   errhint("More details may be available in the server log.")));
		if (status == BGWH_POSTMASTER_DIED)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				  errmsg("cannot start background processes without postmaster"),
					 errhint("Kill all remaining database processes and restart the database.")));
		Assert(status == BGWH_STARTED);

		wrk->started = true; /* FIXME use BackgroundWorkerHandle */

		rc = WaitLatch(&wrk->backendLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH,
					   0, WAIT_EVENT_JSONBC_DICT_WORKER);
	}
	PG_CATCH();
	{
		DisownLatch(&wrk->backendLatch);
		PG_RE_THROW();
	}
	PG_END_TRY();

	DisownLatch(&wrk->backendLatch);

	if (rc & WL_POSTMASTER_DEATH)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			  errmsg("cannot start background processes without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));

	MemoryContextSwitchTo(oldcontext);

	return wrk;
}

#define PG_JSONBC_DICT_LOCK_MAGIC 0x3C59A016

static void
jsonbcDictWorkerInitLockTag(LOCKTAG *tag, Oid dbid)
{
	tag->locktag_field1 = PG_JSONBC_DICT_LOCK_MAGIC;
	tag->locktag_field2 = (uint32) dbid;
	tag->locktag_field3 = 0;
	tag->locktag_field4 = 0;
	tag->locktag_type = LOCKTAG_USERLOCK;
	tag->locktag_lockmethodid = USER_LOCKMETHOD;
}

JsonbcKeyId
jsonbcDictWorkerGetIdByName(JsonbcDictId dict, JsonbcKeyName key,
							JsonbcKeyId nextKeyId)
{
	LOCKTAG					tag;
	JsonbcDictWorker	   *wrk;
	JsonbcKeyId				result;
	char				   *errmsg;
	int					    errmsglen;

	jsonbcDictWorkerInitLockTag(&tag, MyDatabaseId);
	LockAcquire(&tag, ExclusiveLock, false, false);

	wrk = jsonbcDictWorkerStart(MyDatabaseId);

	OwnLatch(&wrk->backendLatch);

	PG_TRY();
	{
		wrk->request.dict = dict;
		wrk->request.nextKeyId = nextKeyId;
		wrk->request.keymq = shm_mq_create(wrk->mqbuf, sizeof(wrk->mqbuf));

		ResetLatch(&wrk->backendLatch);
		SetLatch(&wrk->workerLatch);

		jsonbcDictWorkerSendString(wrk->request.keymq, key.s, key.len);

		(void) WaitLatch(&wrk->backendLatch, WL_LATCH_SET, 0,
						 WAIT_EVENT_JSONBC_DICT_WORKER);

		result = wrk->response.id;

		elog(DEBUG1, "received response from jsonbc worker: %d", result);

		if (result == JsonbcInvalidKeyId)
			errmsg = jsonbcDictWorkerReceiveString(wrk->response.errmq, &errmsglen);

	}
	PG_CATCH();
	{
		DisownLatch(&wrk->backendLatch);
		PG_RE_THROW();
	}
	PG_END_TRY();

	DisownLatch(&wrk->backendLatch);
	LockRelease(&tag, ExclusiveLock, false);

	if (result == JsonbcInvalidKeyId)
		elog(ERROR, "failed to insert key into jsonbc dictionary (%.*s)",
			 errmsglen, errmsg);

	return result;
}
