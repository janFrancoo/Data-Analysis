import requests
import atexit
import csv
import time
import random
import pandas as pd
from collections import defaultdict

session_file = "session.txt"
base_url = "https://api.stackexchange.com/2.2/"
page_count = 1

proxies = [None]
with open("proxies.txt", "r", encoding="utf-8") as file:
    for line in file:
        components = line.strip("\n").split(":")
        proxy = "http://" + components[2] + ":" + components[3] + "@" + components[0] + ":" + components[1]
        proxies.append({
            "http": proxy,
            "https": proxy
        })

current_proxy_idx = 0
has_more = True


def query(func):
    def inner(*args, **kwargs):
        global current_proxy_idx
        while True:
            status_code, response_json = func(*args, **kwargs, proxy=current_proxy_idx)
            if not 200 <= status_code < 300:
                current_proxy_idx += 1
                print(response_json, current_proxy_idx)
            else:
                break
    return inner


@query
def get_questions(page=1, page_size=100, proxy=0):
    global has_more
    get_questions_end = base_url + "questions?page={}&pagesize={}&fromdate=1546300800&todate=1577750400&order=desc" \
                                   "&sort=creation&site=stackoverflow".format(page, page_size)
    try:
        response = requests.get(get_questions_end, proxies=proxies[proxy])
    except requests.exceptions.ConnectionError as e:
        return 0, e
    response_json = response.json()
    if 200 <= response.status_code < 300:
        items = response_json["items"]
        has_more = response_json["has_more"]
        write_questions_to_csv(items)
    return response.status_code, response_json


def write_questions_to_csv(questions):
    with open('questions.csv', mode='a+', newline='') as questions_csv:
        question_writer = csv.writer(questions_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for question in questions:
            question_writer.writerow([question["question_id"] if "question_id" in question else 0,
                                      question["is_answered"] if "is_answered" in question else False,
                                      question["owner"]["user_id"] if "user_id" in question["owner"] else 0,
                                      question["tags"] if "tags" in question else []])


@query
def get_answers(question_ids, page_size=100, proxy=0):
    get_answers_end = base_url + "questions/{}/answers?pagesize={}&order=desc&sort=creation&site=stackoverflow"\
        .format(';'.join(question_ids), page_size)
    response = requests.get(get_answers_end, proxies=proxies[proxy])
    response_json = response.json()
    if 200 <= response.status_code < 300:
        items = response_json["items"]
        question_answered_users_dict = defaultdict(list)
        for item in items:
            question_id = item["question_id"]
            user_id = item["owner"]["user_id"] if "user_id" in item["owner"] else 0
            question_answered_users_dict[question_id].append(user_id)
        for question in question_answered_users_dict:
            write_user_ids_to_csv(question, question_answered_users_dict[question])
    return response.status_code, response_json


def write_user_ids_to_csv(question_id, answered_user_ids):
    with open('answers.csv', mode='a+', newline='') as answers_csv:
        answer_writer = csv.writer(answers_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        answer_writer.writerow([question_id, answered_user_ids])


@query
def get_user_tags(user_ids, page_size=100, proxy=0):
    get_users_tags_end = base_url + "users/{}/tags?pagesize={}&order=desc&min=15&sort=popular&site=stackoverflow"\
        .format(';'.join(user_ids), page_size)
    response = requests.get(get_users_tags_end)
    response_json = response.json()
    if 200 <= response.status_code < 300:
        items = response_json["items"]
        users_tags_dict = defaultdict(list)
        for item in items:
            user_id = item["user_id"]
            tag_name = item["name"]
            users_tags_dict[user_id].append(tag_name)
        for user in users_tags_dict:
            write_users_tags_to_csv(user, users_tags_dict[user])
    return response.status_code, response_json


def write_users_tags_to_csv(user_id, tags):
    with open('user_tags.csv', mode='a+', newline='') as user_tags_csv:
        tag_writer = csv.writer(user_tags_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        tag_writer.writerow([user_id, tags])


def questions():
    page_count = read_session()
    print("program is starting from", page_count)
    atexit.register(exit_handler)

    while True:
        get_questions(page=page_count)
        if not has_more:
            print("No more...")
            break
        page_count += 1
        if page_count % 10 == 0:
            print(page_count)


def answers():
    page_count = read_session()
    print("program is starting from", page_count)
    atexit.register(exit_handler)

    # total questions -> 1799655, answered questions -> 1056003
    questions = pd.read_csv("questions.csv")
    answered_questions = questions[(questions["IsAnswered"] == True) & (questions["UserID"] != 0)]
    size = answered_questions.shape[0]

    start = page_count
    question_ids = []
    for row in answered_questions.iloc[start:].values:
        question_id = row[0]
        question_ids.append(str(question_id))

        if len(question_ids) == 20:
            get_answers(question_ids)
            page_count += 20
            question_ids = []
            time.sleep(0.1)
            if page_count % 100 == 0:
                print(size, page_count)


def users_tags():
    page_count = read_session()
    print("program is starting from", page_count)
    atexit.register(exit_handler)

    # total questions -> 1799655, answered questions -> 1056003
    # 1771573
    questions = pd.read_csv("questions.csv")
    questions = questions[questions["UserID"] != 0]
    unique_users = questions["UserID"].unique()

    size = len(unique_users)
    start = page_count
    user_ids = []
    for user_id in unique_users[start:]:
        user_ids.append(str(user_id))

        if len(user_ids) == 25:
            get_user_tags(user_ids)
            page_count += 20
            user_ids = []
            time.sleep(random.random(0.5, 1.5))
            if page_count % 100 == 0:
                print(size, page_count)


def read_session():
    try:
        file = open(session_file, 'r')
        value = int(file.readline())
        file.close()
        return value
    except FileNotFoundError:
        return 1


def exit_handler():
    file = open(session_file, 'w+')
    file.write(str(page_count))
    file.close()


if __name__ == "__main__":
    questions()
    answers()
    users_tags()
