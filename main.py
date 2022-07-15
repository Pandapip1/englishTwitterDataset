import re
import time

import requests
import os
import json

bearer_token = os.environ.get("TW_BEARER_TOKEN")
start_requests = time.time()
end_requests = time.time()


def parse_tweet(tw):
    if "referenced_tweets" in tw and \
            any(["type" in r_tw and r_tw["type"] == "retweeted" for r_tw in tw["referenced_tweets"]]):
        return ""

    if tw["text"].lower().startswith("rt @"):
        return ""

    if "lang" not in tw or tw["lang"] != "en":
        return ""

    tw_tx = tw["text"]

    # Lowercase it so it's more normalized
    tw_tx = tw_tx.lower()

    # Remove non-space whitespace & duplicated spaces
    tw_tx = re.sub(r'\s+', ' ', tw_tx)

    # Remove non-glyph characters except for # and @ so it's more normalized
    tw_tx = re.sub(r'[^a-zA-Z0-9_#@ ]', '', tw_tx)

    # Remove reply pings
    while tw_tx.startswith("@"):
        tw_tx = " ".join([ngram for ngram in tw_tx.split(" ")][1:])

    # Remove urls
    tw_tx = " ".join([ngram for ngram in tw_tx.split(" ") if not ngram.startswith("http")])

    # Remove duplicated spaces
    tw_tx = " ".join([n for n in tw_tx.split(" ") if len(n) > 0])

    return tw_tx


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2SampledStreamPython"
    return r


def create_stream_url():
    return f"https://api.twitter.com/2/tweets/sample/stream?expansions=author_id&user.fields=id,username&tweet.fields=id,text,lang,referenced_tweets"


# def create_user_tweets_url(user_handle):
#     return f"https://api.twitter.com/2/tweets/search/recent?query=from:{user_handle}&tweet.fields=id,text,lang,referenced_tweets"


def create_user_tweets_url(user_handle):
    return f"https://api.twitter.com/2/users/{user_handle}/tweets?tweet.fields=id,text,lang&max_results=100"


def create_user_tweets_url_token(user_handle, pagination_token):
    return f"https://api.twitter.com/2/users/{user_handle}/tweets?tweet.fields=id,text,lang&max_results=100&pagination_token={pagination_token}"


def stream_new_tweets():
    global start_requests, end_requests
    streaming_response = requests.request("GET", create_stream_url(), auth=bearer_oauth, stream=True)
    if streaming_response.status_code != 200:
        print(f"STATUS CODE {streaming_response.status_code}!!!")
        print(streaming_response.text)
        return
    with open("output.csv", "a") as file:
        for response_line in streaming_response.iter_lines():
            if response_line:
                detected_users = [u["id"] for u in json.loads(response_line)["includes"]["users"]]

                for user in detected_users:
                    user_tweet_response = requests.request("GET", create_user_tweets_url(user), auth=bearer_oauth)
                    while user_tweet_response.status_code == 429:
                        print(user_tweet_response.text)
                        end_requests = time.time()
                        waitfor = 15*60 - (end_requests - start_requests) + 15
                        while waitfor > 0:
                            print(f"Sleeping for {int(waitfor/60)} mins {int(waitfor % 60)} secs")
                            if waitfor > 60:
                                time.sleep(60)
                                waitfor -= 60
                            else:
                                time.sleep(waitfor)
                                waitfor = -1
                        start_requests = time.time()
                        user_tweet_response = requests.request("GET", create_user_tweets_url(user), auth=bearer_oauth)
                    if user_tweet_response.status_code != 200:
                        print(f"STATUS CODE {user_tweet_response.status_code}!!!")
                        print(streaming_response.text)
                        return
                    if "data" not in user_tweet_response.json():  # 0 responses
                        continue
                    tweets = user_tweet_response.json()["data"][:]
                    while "meta" in user_tweet_response.json() and "next_token" in user_tweet_response.json()["meta"]:
                        next_token = user_tweet_response.json()["meta"]["next_token"]
                        user_tweet_response = requests.request("GET", create_user_tweets_url_token(user, next_token), auth=bearer_oauth)
                        if user_tweet_response.status_code == 429:
                            break
                        if user_tweet_response.status_code != 200:
                            print(f"STATUS CODE {user_tweet_response.status_code}!!!")
                            print(streaming_response.text)
                            return
                        if "data" not in user_tweet_response.json():  # 0 responses
                            break
                        tweets.extend(user_tweet_response.json()["data"])
                    write_newline = False
                    for tw in tweets:
                        tw_tx = parse_tweet(tw)
                        # If processing results in no tweet, ignore it
                        if len(tw_tx) == 0:
                            continue
                        print(tw_tx)
                        file.write("\""+tw_tx+"\",")
                        write_newline = True
                    if write_newline:
                        file.write("\n")


def main():
    timeout = 1
    while True:
        stream_new_tweets()
        time.sleep(timeout)
        timeout *= 2

if __name__ == "__main__":
    main()
