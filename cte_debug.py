#!/usr/bin/env python3
from random import randint
import sys
import re

import psycopg2
import psycopg2.extras
from tabulate import tabulate

import config


def execute_partial_cte(sql_query, arguments, query_count):
    """ Execute part of a CTE.

        params: query - the query to execute
        arguments: the full arguments for the full query
        query_count: the number of sub_queries to run. 0 for all, or 2+ for a partial query.
    """

    if query_count == 1:
        print("Query count must be 0 or 2+")
        return

    if query_count == 0:
        final_query = sql_query
        final_args = arguments
    else:
        parens = 0
        sub_query = ""
        sub_queries = []
        for ch in sql_query:
            if ch == "(":
                parens += 1

            if ch == ")":
                parens -= 1
                if parens == 0:
                    sub_query += ch
                    sub_queries.append({"query": sub_query})
                    sub_query = ""
                    continue

            sub_query += ch

        sub_queries.append({"query": sub_query})

        sub_queries[0]["query"] += sub_queries[1]["query"]
        del sub_queries[1]

        for sub_query in sub_queries:
            sub_query["arg_count"] = sub_query["query"].count("%s")
            sub_query["arguments"] = arguments[:sub_query["arg_count"]]
            for i in range(sub_query["arg_count"]):
                arguments.pop(0)

        last = sub_queries[query_count - 1]
        sub_queries.pop(query_count)

        last["query"] = re.sub(r',[^(]+?\(', '', last["query"], count=1).strip()
        if last["query"][-1] == ')':
            last["query"] = last["query"][:-1]

        final_query = ""
        final_args = []
        for q in sub_queries[:query_count - 1]:
            final_query += q["query"]
            final_args.extend(q["arguments"])

        final_query += " " + last["query"]
        final_args.extend(last["arguments"])

    first_row = None
    with psycopg2.connect(config.CONNECT_URI) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
            curs.execute(final_query, tuple(final_args))
            data = []
            for row in curs:
                if first_row is None:
                    first_row = dict(row)
                data.append(list(row))

    return data, list(first_row.keys())


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Usage %s: <num subqueries: 0 or 2+> [explain: 0 or 1]" % sys.argv[0])
        sys.exit(-1)

    num_queries = int(sys.argv[1])
    if len(sys.argv) == 3 and int(sys.argv[2]):
        explain = True
        query = "EXPLAIN " + config.QUERY
    else:
        explain = False
        query = config.QUERY

    rows, headers = execute_partial_cte(query, config.QUERY_PARAMS, num_queries)
    if explain:
        from icecream import ic
        for row in rows:
            print(row[0])
    else:
        tab = [headers]
        tab.extend(rows)
        print(tabulate(tab))
