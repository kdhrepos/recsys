import pymysql
import redis as r
import pandas as pd
from sqlalchemy import create_engine


class Connector:
    def __init__(self):
        self.mysql = create_engine(
            'mysql+pymysql://kim:qwe123@165.229.86.160:8194/cns')
        self.redis = r.Redis(host='localhost', port=6379, db=0)

    def get_member_info(self, member_id):
        member_sql = f"SELECT introduction, position FROM member WHERE id = {member_id};"
        member = pd.read_sql(member_sql, conn.mysql)
        return member

    def get_post_like(self, member_id):
        post_like_sql = f"SELECT p.id, p.content FROM post p JOIN(SELECT post_id FROM post_like WHERE member_id={member_id} ORDER BY id DESC LIMIT 5) AS pl ON p.id = pl.post_id "
        post_like = pd.read_sql(post_like_sql, conn.mysql)
        return post_like

    def get_post(self, member_id):
        post_sql = f"SELECT \
        p.id, \
        p.member_id AS memberId, \
        m.nickname, \
        m.url AS profile, \
        p.content AS content, \
        p.created_at AS createdAt, \
        p.like_cnt AS likeCnt, \
        p.file_cnt AS fileCnt, \
        p.is_comment_enabled AS isCommentEnabled, \
        (SELECT COUNT(*) FROM comment c WHERE c.post_id=p.id) AS commentCnt, \
        EXISTS( \
        SELECT 1  \
        FROM post_like pl  \
        WHERE pl.post_id = p.id  \
        AND pl.member_id = {member_id} \
        ) AS liked \
        FROM \
        post p \
        JOIN \
        member m ON p.member_id = m.id;"
        post = pd.read_sql(post_sql, conn.mysql)

        post['isCommentEnabled'] = post['isCommentEnabled'].astype(
            bool)

        transformed_post = []
        for _, row in post.iterrows():
            transformed_post.append({
                "id": row['id'],
                "postMember": {
                    "id": row['memberId'],
                    "nickname": row['nickname'],
                    "profile": row['profile']
                },
                "content": row['content'],
                "createdAt": row['createdAt'],
                "likeCnt": row['likeCnt'],
                "fileCnt": row['fileCnt'],
                "commentCnt": row['commentCnt'],
                "isCommentEnabled": row['isCommentEnabled'],
                "liked": row['liked']
            })
        post = pd.json_normalize(transformed_post)
        return post

    def get_hashtag_id(self, hashtag):
        hashtag_id_sql = f"SELECT id FROM hashtag WHERE name = \"{hashtag}\""
        hashtag_id = pd.read_sql(hashtag_id_sql, conn.mysql).iloc[0, 0]
        return hashtag_id

    def get_hashtag_post(self, member_id, hashtag_id):
        hashtag_post_sql = f"SELECT \
        p.id, \
        p.member_id AS memberId, \
        m.nickname, \
        m.url AS profile, \
        p.content AS content, \
        p.created_at AS createdAt, \
        p.like_cnt AS likeCnt, \
        p.file_cnt AS fileCnt, \
        p.is_comment_enabled AS isCommentEnabled, \
        (SELECT COUNT(*) FROM comment c WHERE c.post_id=p.id) AS commentCnt, \
        EXISTS( \
        SELECT 1  \
        FROM post_like pl  \
        WHERE pl.post_id = p.id  \
        AND pl.member_id = {member_id} \
        ) AS liked \
        FROM \
            hashtag_post htp \
        JOIN  \
            post p ON htp.post_id = p.id \
        JOIN \
            member m ON p.member_id = m.id \
        WHERE \
            htp.hashtag_id = {hashtag_id} \
        LIMIT 10;"
        hashtag_post = pd.read_sql(hashtag_post_sql, conn.mysql)

        hashtag_post['isCommentEnabled'] = hashtag_post['isCommentEnabled'].astype(
            bool)

        transformed_hashtag_post = []
        for _, row in hashtag_post.iterrows():
            transformed_hashtag_post.append({
                "id": row['id'],
                "postMember": {
                    "id": row['memberId'],
                    "nickname": row['nickname'],
                    "profile": row['profile']
                },
                "content": row['content'],
                "createdAt": row['createdAt'],
                "likeCnt": row['likeCnt'],
                "fileCnt": row['fileCnt'],
                "commentCnt": row['commentCnt'],
                "isCommentEnabled": row['isCommentEnabled'],
                "liked": row['liked']
            })
        hashtag_post = pd.json_normalize(transformed_hashtag_post)
        return hashtag_post


conn = Connector()
