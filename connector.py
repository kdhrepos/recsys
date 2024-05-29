import pymysql
import redis as r
import pandas as pd


class Connector:
    def __init__(self):
        self.mysql = pymysql.connect(host='165.229.86.160', user='kim', port=8194,
                                     password='qwe123', db='cns', charset='utf8')
        self.redis = r.Redis(host='localhost', port=6379, db=0)

    def get_member_info(self, member_id):
        member_sql = f"SELECT introduction, position FROM member WHERE id = {member_id};"
        member = pd.read_sql(member_sql, conn.mysql)
        return member

    def get_post_like(self, member_id):
        post_like_sql = f"SELECT p.id, p.content FROM post p JOIN(SELECT post_id FROM post_like WHERE member_id={member_id} ORDER BY id DESC LIMIT 5) AS pl ON p.id = pl.post_id "
        post_like = pd.read_sql(post_like_sql, conn.mysql)
        return post_like

    def get_post(self):
        post_sql = "SELECT * FROM post;"
        post = pd.read_sql(post_sql, conn.mysql)
        return post


conn = Connector()
