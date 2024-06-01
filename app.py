import json
import time
import numpy as np

from gensim.models import FastText
from flask import Flask, jsonify, Response
from connector import conn
from recommender import get_post_recommendations
from dto import post_response

app = Flask(__name__)
fast_model = FastText.load("./fasttext_reduced_model.model")


@app.route('/post-recommendation/<member_id>/<page_num>/<page_size>')
def post_recommendation(member_id, page_num, page_size):
    page_size = int(page_size)
    post_list = conn.get_post(member_id)

    # 이미 반환된 게시글이 있는지 검색
    returned_posts = conn.redis.hget(f'p_{member_id}', page_num)

    # 같은 페이지 새로고침 했다면 그냥 그 페이지들 그대로 반환
    if returned_posts:
        # timeout 갱신
        conn.redis.expire(name=f"p_{member_id}", time=3600)
        returned_posts = json.loads(returned_posts.decode())
        post_list = post_list[post_list['id'].isin(
            returned_posts)]
        return Response(post_list.to_json(orient='records', date_format='iso', force_ascii=False), content_type='application/json')
    # 새로운 페이지 요청이라면, 제외할 페이지 없음
    else:
        returned_posts = []

    # 반환되었던 게시글 모두 읽어옴
    returned_posts = conn.redis.hvals(f'p_{member_id}')
    returned_posts = [json.loads(posts.decode()) for posts in returned_posts]
    returned_posts = np.array(returned_posts).flatten().tolist()

    # 이미 반환했던 게시글은 제외
    post_list = post_list[~post_list['id'].isin(returned_posts)]

    # 현재 존재하는 게시글의 개수가 요청한 페이지 크기보다 작거나 같다면
    # 즉, 마지막 페이지인 경우
    if (len(post_list) <= page_size):
        return Response(post_list.to_json(orient='records', date_format='iso',))

    post_like = conn.get_post_like(member_id)
    member_info = conn.get_member_info(member_id)

    recommended_posts = get_post_recommendations(
        fast_model, post_like, post_list, member_info, page_num, page_size)

    # recommended_posts를 list로 변환
    recommended_posts_saved = recommended_posts.id.tolist()

    # redis에 반환된 게시글 저장하여 중복 게시글 방지
    conn.redis.hset(name=f"p_{member_id}", key=page_num,
                    value=json.dumps(recommended_posts_saved))

    # DataFrame을 JSON 형식으로 변환 후 반환
    return Response(recommended_posts.to_json(orient='records', date_format='iso',),
                    mimetype='application/json')


@app.route('/hashtag-recommendation/<hashtag>/<member_id>/<page_num>/<page_size>')
def hashtag_recommendation(hashtag, member_id, page_num, page_size):
    page_size = int(page_size)
    hashtag_id = conn.get_hashtag_id(hashtag)
    hashtag_post_list = conn.get_hashtag_post(member_id, hashtag_id)

    # 이미 반환된 게시글이 있는지 검색
    returned_posts = conn.redis.hget(f'h_{hashtag_id}_{member_id}', page_num)

    # 같은 페이지 새로고침 했다면 그냥 그 페이지들 그대로 반환
    # timeout 갱신
    if returned_posts:
        conn.redis.expire(name=f"h_{hashtag_id}_{member_id}", time=3600)
        returned_posts = json.loads(returned_posts.decode())
        hashtag_post_list = hashtag_post_list[hashtag_post_list['id'].isin(
            returned_posts)]

        return Response(hashtag_post_list.to_json(orient='records', date_format='iso',),
                        mimetype='application/json')
    # 새로운 페이지 요청이라면, 제외할 페이지 없음
    else:
        returned_posts = []

    # 반환되었던 게시글 모두 읽어옴
    returned_posts = conn.redis.hvals(f'h_{hashtag}_{member_id}')
    returned_posts = [json.loads(posts.decode()) for posts in returned_posts]
    returned_posts = np.array(returned_posts).flatten().tolist()

    hashtag_post_list = hashtag_post_list[~hashtag_post_list['id'].isin(
        returned_posts)]

    if (len(hashtag_post_list) <= page_size):
        return Response(hashtag_post_list.to_json(orient='records', date_format='iso',),
                        mimetype='application/json')

    post_like = conn.get_post_like(member_id)
    member_info = conn.get_member_info(member_id)

    recommended_posts = get_post_recommendations(
        fast_model, post_like, hashtag_post_list, member_info, page_num, page_size)

    # recommended_posts의 postId를 list로 변환
    saved_recommended_posts = recommended_posts.id.tolist()

    # redis에 반환된 게시글 저장하여 중복 게시글 방지
    conn.redis.hset(name=f"h_{hashtag_id}_{member_id}", key=page_num,
                    value=json.dumps(saved_recommended_posts))

    # DataFrame을 JSON 형식으로 변환 후 반환
    return Response(recommended_posts.to_json(orient='records', date_format='iso',),
                    mimetype='application/json')


@ app.route('/job-recommendation/<member_id>')
def job_recommendation(member_id):
    NotImplemented


if __name__ == '__main__':
    # app.run(host='0.0.0.0')
    app.run(debug=True)
