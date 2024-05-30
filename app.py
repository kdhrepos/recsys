import json
import time
from gensim.models import FastText
from flask import Flask, jsonify
from connector import conn
from post_recommender import get_post_recommendations

app = Flask(__name__)
fast_model = FastText.load("./fasttext_model.model")


@app.route('/post-recommendation/<member_id>/<page_num>/<token>')
def post_recommendation(member_id, page_num, token):
    # started = time.time()
    posts = conn.get_post()
    post_like = conn.get_post_like(member_id)
    member_info = conn.get_member_info(member_id)

    returned_posts = conn.redis.get(name=f"p_{token}_{page_num}")
    # 같은 페이지 새로고침 했다면 그냥 그 페이지들 그대로 반환
    # timeout 갱신해야 함
    if returned_posts:
        conn.redis.expire(name=f"p_{token}_{page_num}", time=3600)
        returned_posts = json.loads(returned_posts.decode())
        return jsonify(returned_posts)
    # 새로운 페이지 요청이라면, 제외할 페이지 없음
    else:
        returned_posts = []

    # 이미 반환했던 게시글은 제외
    posts = posts[~posts['id'].isin(returned_posts)]

    recommended_posts = get_post_recommendations(
        fast_model, post_like, posts, member_info, page_num)

    # recommended_posts를 list로 변환
    recommended_posts = recommended_posts.tolist()

    # redis에 반환된 게시글 저장하여 중복 게시글 방지
    conn.redis.set(name=f"p_{token}_{page_num}",
                   value=json.dumps(recommended_posts), ex=3600)

    # print(f"Time : {time.time() - started}")

    # DataFrame을 JSON 형식으로 변환 후 반환
    return jsonify(recommended_posts)


@app.route('/hashtag-recommendation/<hashtag>')
def hashtag_recommendation(hashtag):
    NotImplemented


@app.route('/job-recommendation/<member_id>')
def job_recommendation(member_id):
    NotImplemented


if __name__ == '__main__':
    app.run(debug=True)
