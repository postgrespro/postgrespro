#include "pathman.h"


// Range
// make_range(int min, int max)
// {
// 	return min << 16 | (max & 0x0000FFFF);
// }

// int
// range_min(IndexRange range)
// {
// 	return range >> 16;
// }

// int
// range_max(IndexRange range)
// {
// 	return range & 0x0000FFFF;
// }

List *
unite_ranges(List *a, List *b)
{
	ListCell *lc;

	foreach(lc, b)
	{
		IndexRange r = (IndexRange)lfirst_int(lc);
		a = append_range(a, r);
	}
	return a;
}

List *
append_range(List *a_lst, IndexRange b)
{
	ListCell *lc;
	int low, high;
	bool merged = false;

	foreach(lc, a_lst)
	{
		// IndexRange &ra = (IndexRange)lfirst_int(lc);
		IndexRange *a = &(lfirst_int(lc));

		/* if ranges overlay then merge them */
		if (range_min(*a) <= range_max(b) && range_max(*a) >= range_min(b))
		{
			low = (range_min(*a) < range_min(b)) ? range_min(*a) : range_min(b);
			high = (range_max(*a) >= range_max(b)) ? range_max(*a) : range_max(b);
			*a = make_range(low, high);
			merged = true;
			break;
		}
	}

	/* if ranges not merged then append range */
	if (!merged)
		a_lst = lappend_int(a_lst, b);

	return a_lst;
}

List *
intersect_ranges(List *a, List *b)
{
	ListCell *lca;
	ListCell *lcb;
	List *new_list = NIL;
	int low, high;

	/* if a or b (or both) are empty then intersections is also empty */
	if (!a || !b)
	{
		if (a)
			pfree(a);
		if (b)
			pfree(b);
		return NIL;
	}

	foreach(lcb, b)
	{
		IndexRange rb = (IndexRange)lfirst_int(lcb);

		/* if "a" is empty then initialize it with infinite range */
		// if (a == NIL)
		// 	a = list_make1_int(make_range(0, RANGE_INFINITY));

		/* intersect entry from "b" and every entry in "a" */
		foreach(lca, a)
		{
			IndexRange ra = (IndexRange)lfirst_int(lca);

			/* if ranges doesn't overlay */
			if (range_min(ra) > range_max(rb) || range_max(ra) < range_min(rb))
				continue;

			low = (range_min(ra) < range_min(rb)) ? range_min(rb) : range_min(ra);
			high = (range_max(ra) >= range_max(rb)) ? range_max(rb) : range_max(ra);

			new_list = lappend_int(new_list, make_range(low, high));
		}
	}
	pfree(a);
	pfree(b);

	return new_list;
}
