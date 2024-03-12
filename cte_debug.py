#!/usr/bin/env python3

import re

query = """WITH mbids(mbid, score) AS (
                               VALUES %s
                           ), similar_artists AS (
                               SELECT CASE WHEN mbid0 = mbid::UUID THEN mbid1::TEXT ELSE mbid0::TEXT END AS similar_artist_mbid
                                    , sa.score
                                    , PERCENT_RANK() OVER (PARTITION BY mbid ORDER BY sa.score) AS rank
                                 FROM similarity.artist sa
                                 JOIN mbids
                                   ON TRUE
                                WHERE (mbid0 = mbid::UUID OR mbid1 = mbid::UUID)
                           ), knockdown AS (
                               SELECT similar_artist_mbid
                                    , CASE WHEN similar_artist_mbid = oa.artist_mbid::TEXT THEN score * oa.factor ELSE score END AS score   
                                    , rank
                                 FROM similar_artists sa
                            LEFT JOIN similarity.overhyped_artists oa
                                   ON sa.similar_artist_mbid = oa.artist_mbid::TEXT
                             ORDER BY score DESC
                                LIMIT %s
                           ), select_similar_artists AS (
                               SELECT similar_artist_mbid
                                    , score
                                 FROM knockdown
                                WHERE rank >= %s and rank < %s
                                ORDER BY score
                                LIMIT %s
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
                                    , PERCENT_RANK() OVER (PARTITION BY similar_artist_mbid ORDER BY sa.similar_artist_mbid, total_listen_count ) AS rank
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

#    for query in sub_queries[:query_count-1]:
#        print("%d ->%s<- (%s)" % (query["arg_count"], query["query"], ",".join([ str(a) for a in query["arguments"]])))
#        print()

    last = sub_queries[query_count]
    sub_queries.pop(query_count)

    last["query"] = re.sub(r',[^(]+\(', '', last["query"])

    final_query = " ".join([ q["query"] for q in sub_queries[:query_count-1]]) + " " + last["query"]
    print(final_query)


if __name__ == "__main__":
    execute_partial_cte(query, ["8f6bd1e4-fbe1-4f50-aa9b-94c450ec0f11", 15, .7, .8, 8, .7, .8, 30], 4)
