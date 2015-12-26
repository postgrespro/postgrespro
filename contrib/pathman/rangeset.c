#include "pathman.h"

bool
irange_intersects(IndexRange a, IndexRange b)
{
	return (irange_lower(a) <= irange_upper(b)) ||
		   (irange_lower(b) <= irange_upper(a));
}

bool
irange_conjuncted(IndexRange a, IndexRange b)
{
	return (irange_lower(a) - 1 <= irange_upper(b)) ||
		   (irange_lower(b) - 1 <= irange_upper(a));
}

IndexRange
irange_union(IndexRange a, IndexRange b)
{
	Assert(irange_is_lossy(a) == irange_is_lossy(b));
	return make_irange(Min(irange_lower(a), irange_lower(b)),
					   Max(irange_upper(a), irange_upper(b)),
					   irange_is_lossy(a));
}

IndexRange
irange_intersect(IndexRange a, IndexRange b)
{
	return make_irange(Max(irange_lower(a), irange_lower(b)),
					   Min(irange_upper(a), irange_upper(b)),
					   irange_is_lossy(a) || irange_is_lossy(b));
}

List *
irange_list_union(List *a, List *b)
{
	ListCell   *ca,
			   *cb;
	List	   *result = NIL;
	IndexRange	cur;
	bool		have_cur = false;

	ca = list_head(a);
	cb = list_head(b);

	while (ca || cb)
	{
		IndexRange	next;

		if (ca && cb)
		{
			if (irange_lower(lfirst_irange(ca)) <= irange_lower(lfirst_irange(cb)))
				next = lfirst_irange(ca);
			else
				next = lfirst_irange(cb);
		}
		else if (ca)
		{
			next = lfirst_irange(ca);
		}
		else if (cb)
		{
			next = lfirst_irange(cb);
		}

		if (!have_cur)
		{
			cur = next;
			have_cur = true;
		}
		else
		{
			if (irange_conjuncted(next, cur))
			{
				if (irange_is_lossy(next) == irange_is_lossy(cur))
				{
					cur = irange_union(next, cur);
				}
				else
				{
					if (!irange_is_lossy(cur))
					{
						result = lappend_irange(result, cur);
						cur = make_irange(irange_upper(cur) + 1,
												 irange_upper(next),
												 irange_is_lossy(next));
					}
					else
					{
						result = lappend_irange(result, 
									make_irange(irange_lower(cur),
												irange_lower(next) - 1,
												irange_is_lossy(cur)));
						cur = next;
					}
				}
			}
			else
			{
				result = lappend_irange(result, cur);
				cur = next;
			}
		}
	}

	if (have_cur)
		result = lappend_irange(result, cur);

	return result;
}

List *
irange_list_intersect(List *a, List *b)
{
	ListCell   *ca,
			   *cb;
	List	   *result = NIL;
	IndexRange	ra, rb;

	ca = list_head(a);
	cb = list_head(b);

	while (ca && cb)
	{
		ra = lfirst_irange(ca);
		rb = lfirst_irange(cb);
		if (irange_intersects(ra, rb))
			result = lappend_irange(result, irange_intersect(ra, rb));

		if (irange_lower(ra) <= irange_lower(rb))
			ca = lnext(ca);
		else
			cb = lnext(cb);
	}
	return result;
}
