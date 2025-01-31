import json
import numpy as np
import pandas as pd

from gensim.models import FastText
from flask import Flask, jsonify, Response
from connector import conn
from recommender import get_post_recommendations

app = Flask(__name__)
fast_model = FastText.load("./fasttext_reduced_model.model")


@app.route('/post-recommendation/<member_id>/<page_num>/<page_size>')
def post_recommendation(member_id, page_num, page_size):
    try:
        page_size = int(page_size)
        page_num = int(page_num)
        post_vector_list = conn.get_post_vector()
        post_vector_list_len = len(post_vector_list)

        # 페이지 번호가 범위를 넘어선 경우
        max_page_num = post_vector_list_len // page_size
        if (post_vector_list_len % page_size != 0):
            max_page_num += 1
        if (max_page_num < page_num):
            return []

        # 이미 반환된 게시글이 있는지 검색
        returned_posts = conn.redis.hget(f'p_{member_id}', page_num)

        # 같은 페이지 새로고침 했다면 그냥 그 페이지들 그대로 반환
        if returned_posts:
            # timeout 갱신
            conn.redis.expire(name=f"p_{member_id}", time=3600)
            returned_posts = returned_posts.decode()
            returned_posts = "(" + returned_posts[1:-1] + ")"
            post_list = conn.get_post_by_id(member_id, returned_posts)
            return Response(post_list.to_json(orient='records', date_format='iso', force_ascii=False), mimetype='application/json')
        # 새로운 페이지 요청이라면, 제외할 페이지 없음
        else:
            returned_posts = []

        # 반환되었던 게시글 모두 읽어옴
        returned_posts = conn.redis.hvals(f'p_{member_id}')
        returned_posts = [json.loads(posts.decode())
                          for posts in returned_posts]
        returned_posts = np.array(returned_posts).flatten().tolist()

        # 이미 반환했던 게시글은 제외
        post_vector_list = post_vector_list[~post_vector_list['post_id'].isin(
            returned_posts)]

        # 현재 존재하는 게시글의 개수가 요청한 페이지 크기보다 작거나 같다면
        # 즉, 마지막 페이지인 경우
        if (len(post_vector_list) <= page_size):
            post_id_list = "(" + \
                ", ".join(map(str, post_vector_list['post_id'])) + ")"
            post_list = conn.get_post_by_id(member_id, post_id_list)
            return Response(post_list.to_json(orient='records', date_format='iso',), mimetype='application/json')

        post_like_vector_list = conn.get_post_like_vector(member_id)
        member_info = conn.get_member_info(member_id)

        recommended_post_list = get_post_recommendations(
            fast_model, post_like_vector_list, post_vector_list, member_info, page_num, page_size)

        # recommended_post_list를 list로 변환
        saving_recommended_post_list = recommended_post_list.tolist()

        # redis에 반환된 게시글 저장하여 중복 게시글 방지
        conn.redis.hset(name=f"p_{member_id}", key=page_num,
                        value=json.dumps(saving_recommended_post_list))

        post_id_list = "(" + \
            ", ".join(map(str, recommended_post_list)) + ")"
        recommended_post_list = conn.get_post_by_id(member_id, post_id_list)

        # DataFrame을 JSON 형식으로 변환 후 반환
        return Response(recommended_post_list.to_json(orient='records', date_format='iso',),
                        mimetype='application/json')
    except Exception as e:
        print(f"Error : {e}")


@app.route('/hashtag-recommendation/<hashtag_id>/<member_id>/<page_num>/<page_size>')
def hashtag_recommendation(hashtag_id, member_id, page_num, page_size):
    try:
        page_size = int(page_size)
        page_num = int(page_num)
        hashtag_post_vector_list = conn.get_hashtag_post_vector(hashtag_id)
        hashtag_post_vector_list_len = len(hashtag_post_vector_list)

        # 페이지 번호가 범위를 넘어선 경우
        max_page_num = hashtag_post_vector_list_len // page_size
        if (hashtag_post_vector_list_len % page_size != 0):
            max_page_num += 1
        if (max_page_num < page_num):
            return []

        # 이미 반환된 게시글이 있는지 검색
        returned_post_id_list = conn.redis.hget(
            f'h_{hashtag_id}_{member_id}', page_num)

        # 같은 페이지 새로고침 했다면 그냥 그 페이지들 그대로 반환
        # timeout 갱신
        if returned_post_id_list:
            conn.redis.expire(name=f"h_{hashtag_id}_{member_id}", time=3600)
            returned_post_id_list = returned_post_id_list.decode()
            returned_post_id_list = "(" + returned_post_id_list[1:-1] + ")"
            hashtag_post_list = conn.get_post_by_id(
                member_id, returned_post_id_list)

            return Response(hashtag_post_list.to_json(orient='records', date_format='iso',),
                            mimetype='application/json')
        # 새로운 페이지 요청이라면, 제외할 페이지 없음
        else:
            returned_posts = []

        # 반환되었던 게시글 모두 읽어옴
        returned_posts = conn.redis.hvals(f'h_{hashtag_id}_{member_id}')
        returned_posts = [json.loads(posts.decode())
                          for posts in returned_posts]
        returned_posts = np.array(returned_posts).flatten().tolist()

        hashtag_post_vector_list = hashtag_post_vector_list[~hashtag_post_vector_list['post_id'].isin(
            returned_posts)]

        if (len(hashtag_post_vector_list) <= page_size):
            hashtag_post_id_list = "(" + \
                ", ".join(map(str, hashtag_post_vector_list['post_id'])) + ")"
            hashtag_post_id_list = conn.get_post_by_id(
                member_id, hashtag_post_id_list)
            return Response(hashtag_post_id_list.to_json(orient='records', date_format='iso',),
                            mimetype='application/json')

        post_like_vector_list = conn.get_post_like_vector(member_id)
        member_info = conn.get_member_info(member_id)

        recommended_post_list = get_post_recommendations(
            fast_model, post_like_vector_list, hashtag_post_vector_list, member_info, page_num, page_size)

        # recommended_posts의 postId를 list로 변환
        saving_recommended_post_list = recommended_post_list.tolist()

        # redis에 반환된 게시글 저장하여 중복 게시글 방지
        conn.redis.hset(name=f"h_{hashtag_id}_{member_id}", key=page_num,
                        value=json.dumps(saving_recommended_post_list))

        hashtag_post_id_list = "(" + \
            ", ".join(map(str, recommended_post_list)) + ")"
        recommended_post_list = conn.get_post_by_id(
            member_id, hashtag_post_id_list)

        # DataFrame을 JSON 형식으로 변환 후 반환
        return Response(recommended_post_list.to_json(orient='records', date_format='iso',),
                        mimetype='application/json')
    except Exception as e:
        print(f"Error : {e}")


@ app.route('/job-recommendation/<member_id>')
def job_recommendation(member_id):
    NotImplemented


if __name__ == '__main__':
    app.run(host='0.0.0.0')
    # app.run(debug=True)
