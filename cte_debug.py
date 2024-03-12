#!/usr/bin/env python3
from random import randint
import sys
import re

import psycopg2
import psycopg2.extras
from tabulate import tabulate

import config

query = """WITH mbids(mbid, score) AS (
                               VALUES %s
                           ), similar_artists AS (
                               SELECT CASE WHEN mbid0 = mbid::UUID THEN mbid1::TEXT ELSE mbid0::TEXT END AS similar_artist_mbid
                                    , sa.score
                                    , ROW_NUMBER() OVER (PARTITION BY mbid ORDER BY sa.score DESC) AS rownum
                                 FROM similarity.artist sa
                                 JOIN mbids
                                   ON TRUE
                                WHERE (mbid0 = mbid::UUID OR mbid1 = mbid::UUID)
                           ), knockdown AS (
                               SELECT similar_artist_mbid
                                    , CASE WHEN similar_artist_mbid = oa.artist_mbid::TEXT THEN score * oa.factor ELSE score END AS score   
                                    , rownum
                                 FROM similar_artists sa
                            LEFT JOIN similarity.overhyped_artists oa
                                   ON sa.similar_artist_mbid = oa.artist_mbid::TEXT
                             ORDER BY rownum
                                LIMIT %s
                           ), select_similar_artists AS (
                               SELECT similar_artist_mbid
                                    , score
                                    --, rownum
                                 FROM knockdown
                                WHERE rownum in %s
                                ORDER BY rownum
                           ), similar_artists_and_orig_artist AS (
                               SELECT *
                                 FROM select_similar_artists
                                UNION
                               SELECT *
                                 FROM mbids
                           ), combine_similarity AS (
                               SELECT similar_artist_mbid
                                    , artist_mbid
                                    , recording_mbid
                                    , total_listen_count
                                    , total_user_count
                                 FROM popularity.top_recording tr
                                 JOIN similar_artists_and_orig_artist sao
                                   ON tr.artist_mbid = sao.similar_artist_mbid::UUID
                                UNION ALL
                               SELECT similar_artist_mbid
                                    , artist_mbid
                                    , recording_mbid
                                    , total_listen_count
                                    , total_user_count
                                 FROM popularity.mlhd_top_recording tmr
                                 JOIN similar_artists_and_orig_artist sao2
                                   ON tmr.artist_mbid = sao2.similar_artist_mbid::UUID
                           ), group_similarity AS (
                               SELECT similar_artist_mbid
                                    , artist_mbid
                                    , recording_mbid
                                    , SUM(total_listen_count) AS total_listen_count
                                    , SUM(total_user_count) AS total_user_count
                                 FROM combine_similarity
                             GROUP BY recording_mbid, artist_mbid, similar_artist_mbid
                           ), top_recordings AS (
                               SELECT sa.similar_artist_mbid
                                    , gs.recording_mbid
                                    , total_listen_count
                                    , PERCENT_RANK() OVER (PARTITION BY sa.similar_artist_mbid ORDER BY sa.similar_artist_mbid, total_listen_count ) AS rank
                                 FROM group_similarity gs
                                 JOIN similar_artists_and_orig_artist sa
                                   ON sa.similar_artist_mbid::UUID = gs.artist_mbid
                             GROUP BY sa.similar_artist_mbid, gs.total_listen_count, gs.recording_mbid
                           ), randomize AS (
                               SELECT similar_artist_mbid
                                    , recording_mbid
                                    , total_listen_count
                                    , rank
                                    , ROW_NUMBER() OVER (PARTITION BY similar_artist_mbid ORDER BY RANDOM()) AS rownum 
                                 FROM top_recordings
                                WHERE rank >= %s and rank < %s   -- select the range of results here
                           )
                               SELECT similar_artist_mbid::TEXT
                                    , recording_mbid
                                    , total_listen_count
                                 FROM randomize
                                WHERE rownum < %s"""                                                                                                                                                               

def execute_partial_cte(sql_query, arguments, query_count):

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
        for q in sub_queries[:query_count-1]:
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

    first_data = [ k for k in first_row.keys() ]
    data.insert(0, first_data)
    print(tabulate(data))


if __name__ == "__main__":
    num_queries = int(sys.argv[1])
    max_num_similar_artists = int(sys.argv[2])
    mode = sys.argv[3]

    step_index = { "easy": (2, 0), "medium": (4, 3), "hard": (10, 10) }
    steps, offset = step_index[mode]

    artist_indexes = []
    for i in range(max_num_similar_artists):
        try:
            artist_indexes.append(randint((i * steps + offset), ((i + 1) * steps + offset)))
        except IndexError:
            break

    execute_partial_cte(query, [("8f6bd1e4-fbe1-4f50-aa9b-94c450ec0f11", 0), 100, tuple(artist_indexes), .7, .8, 30], num_queries)
