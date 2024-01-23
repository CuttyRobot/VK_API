import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
import datetime
import time
from cassandra.cluster import Cluster
from uuid import uuid4
import requests

app = FastAPI()

server_url = "https://dn2vgjs6-9443.euw.devtunnels.ms/"

vk_api_token = 'vk1.a.Pjjc8ETB4ACaZtLqirTvvxPlBj0ZvmzAH7j0RKGo8t1sXNaz0-f_fk--BUdVKYMlOyzOEI5pPLraYbcu318AgTGMClMc3DOnhS3K9AwtsRnCIE42RdL-xIi_57Y_l3Uc8SOrdBx-dfrOHjRidpjNZwdlnKfff1IP6T1qYSgGrLPbsrAnpjxHaWkRqSKe8WiOmO6Rg0yjx6KTbShyO8UwAA'
CONFIRMATION_TOKEN = 'a33e9b61'
APPROVE_CODE = 'Astartes22Foundation'
cluster = Cluster(['localhost'], port=60436)
session = cluster.connect()


@app.get("/")
async def approve_point():
    return "Server running..."


@app.get("/get_all_posts")
async def approve_point():
    url = f'https://api.vk.com/method/wall.get?owner_id=-{224247696}&access_token={vk_api_token}&v=5.131'
    data = get_all_posts(vk_api_token, 224247696, '5.131')
    print(data)
    return "Server running..."


@app.get("/all_comments")
async def approve_point():
    # Выборка всех данных из таблицы
    cql_select_query = "SELECT * FROM my_keyspace.comments"
    result = session.execute(cql_select_query)

    # Перебор результатов выборки и вывод на экран
    for row in result:
        print(
            f"Comment ID: {row[0]}, Author ID: {row[1]}, Number of Posts: {row[2]}, Text: {row[3]}, Stack: {row[4]}"
        )
    return "ok"


# Confirmation End-Point
@app.post("/")
async def approve_point(request: Request):
    data = await request.json()
    print('------------------------------------------------------------')
    # Server Confirmation Case
    if data['type'] == 'confirmation' and data['group_id'] == 224247696:
        print("Server Confirmed")
        return PlainTextResponse(CONFIRMATION_TOKEN)

    # Reply to Post Case
    elif data['type'] == 'wall_reply_new' and data['group_id'] == 224247696:
        if not data['object'].get('parents_stack'):
            print("Reply on post:")
            stack = 0
        else:
            print("Reply on comment:")
            print(f"{data['object']['parents_stack']}")
            stack = 5

        print(f"Time received: {time.time()}. Time of action: {data['object']['date']}")
        print(f"Author ID:{data['object']['from_id']}")
        print(f"Reply to Wall Post(ID):{data['object']['post_id']}")
        print(f"Comment Number: {data['object']['id']}")
        print(f"Text: {data['object']['text']}")
        comment_id = uuid4()
        author_id = data['object']['from_id']
        text = data['object']['text']
        number_of_post = data['object']['post_id']

        # Создание запроса на вставку с использованием f-строки
        cql_insert_query = f"""
            INSERT INTO my_keyspace.comments (Comment_ID, Author_ID, Number_of_Post, Text, Stack)
            VALUES ({comment_id}, '{author_id}', {number_of_post}, '{text}', {stack})
        """

        session.execute(cql_insert_query)
        return PlainTextResponse("Ok")

    # New Post Case
    elif data['type'] == 'wall_post_new':
        print("New Post")
        print(f"Time received: {time.time()}. Time of action: {data['object']['date']}")
        alpha = data['object']['from_id']

        if alpha < 0:
            print("Post from Community")
        else:
            print("Post from User")

        print(f"Author ID:{alpha}")
        print(f"Post ID: {data['object']['id']}")
        return PlainTextResponse("Ok")

    # Like Add to Post Case
    elif data['type'] == 'like_add':
        print("Like Add")
        print(f"Time received: {time.time()}.")
        print(f"Author ID: {data['object']['from_id']}")
        print(f"Reply to Wall Post(ID): {data['object']['post_id']}")
        print(f"Comment Number: {data['object']['id']}")
        print(f"Text: {data['object']['text']}")
        return PlainTextResponse("Ok")

    # Like Remove from Post Case
    elif data['type'] == 'like_remove':
        print("Like Removed")
        print(f"Time received: {time.time()}. Time of action: {data['object']['date']}")
        print(f"Author ID:{data['object']['from_id']}")
        print(f"Reply to Wall Post(ID):{data['object']['post_id']}")
        print(f"Comment Number: {data['object']['id']}")
        print(f"Text: {data['object']['text']}")
        return PlainTextResponse("Ok")

    # Unknown Callback Case
    else:
        print("Unknown callback")
        print(data)
        return PlainTextResponse("Ok")


def get_all_posts(access_token, group_id, version):
    all_posts = []
    offset = 0
    count = 100  # максимальное количество постов, которое можно получить за один запрос

    while True:
        response = requests.get(
            'https://api.vk.com/method/wall.get',
            params={
                'access_token': access_token,
                'owner_id': f'-{group_id}',
                'count': count,
                'offset': offset,
                'v': version
            }
        ).json()
        print(response)
        posts = response.get('response', {}).get('items', [])
        if not posts:
            break

        all_posts.extend(posts)
        offset += count

        time.sleep(0.5)  # Соблюдайте ограничения на количество запросов в секунду

    return all_posts


if __name__ == '__main__':
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS my_keyspace
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)

    cql_create_table_query = """
        CREATE TABLE IF NOT EXISTS my_keyspace.comments (
            Comment_ID UUID PRIMARY KEY,
            Author_ID TEXT,
            Number_of_Post INT,
            Text TEXT,
            Stack INT
        )
    """

    session.execute(cql_create_table_query)

    uvicorn.run(app, host='localhost', port=9443)
