import json
import pymysql
import os
import os.path
from PIL import Image
from telethon.sync import TelegramClient
import datetime
from datetime import datetime, date
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty
from tqdm import tqdm
import string
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import time
import re
from config import settings

start_t = time.time()

link = 'https://t.me/EUC_market'
from_chat_id = 1323772109
username = settings.username
api_id = settings.api_id
api_hash = settings.api_hash
phone = settings.phone
folder = "C:/Users/PoPo/PycharmProjects/Site_parser/euc/adsboard/static/adsboard/img/1323772109"

client = TelegramClient(phone, api_id, api_hash)
client.connect()

if not client.is_user_authorized():
    client.send_code_request(phone)
    client.sign_in(phone, input('Enter the code: '))

client.start()


async def dump_all_participants(channel):
    offset_user = 0
    limit_user = 100
    all_participants = []
    filter_user = ChannelParticipantsSearch('')

    while 1:
        participants = await client(GetParticipantsRequest(channel, filter_user, offset_user, limit_user, hash=0))
        if not participants.users:
            break
        all_participants.extend(participants.users)
        offset_user += len(participants.users)

    all_users_details = []

    for participant in all_participants:
        if not participant.bot:
            all_users_details.append({"id": participant.id,
                                      "first_name": participant.first_name,
                                      "last_name": participant.last_name,
                                      "user": participant.username,
                                      "phone": participant.phone,
                                      })

    users_unique = [dict(t) for t in {tuple(d.items()) for d in all_users_details}]

    db_update(users_unique, "user")


async def dump_all_messages(channel):
    offset_msg = 0
    limit_msg = 100
    total_count_limit = 2000
    all_messages = []

    class DateTimeEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, datetime):
                return o.isoformat()
            if isinstance(o, bytes):
                return list(o)
            return json.JSONEncoder.default(self, o)

    while 1:
        history = await client(GetHistoryRequest(
            peer=channel,
            offset_id=offset_msg,
            offset_date=None, add_offset=0,
            limit=limit_msg, max_id=0, min_id=0,
            hash=0))
        if not history.messages:
            break

        messages = history.messages
        for message in messages:
            message = message.to_dict()
            if ('message' in message) and (message['message'] != '') and ('from_id' in message):
                all_messages.append(message)

        offset_msg = messages[len(messages) - 1].id
        total_messages = len(all_messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break

    final_all_messages = []
    print(len(all_messages))

    for i in range(len(all_messages)):
        d = {}
        for a, b in all_messages[i].items():
            keys = ['id', 'date', 'message', 'from_id']
            if (b is not None) and (a in keys):
                if a == 'id':
                    d['id'] = b
                    d['link'] = link + '/' + str(b)
                elif (a == 'from_id') and ('user_id' in all_messages[i][a]):
                    d['from_id'] = all_messages[i][a]['user_id']  # x[0]['a']['b']
                else:
                    d[a] = b
                    d["deleted"] = False
        final_all_messages.append(d)

    with open('files/channel_messages_new.json', 'w', encoding='utf8') as outfile:
        json.dump(final_all_messages, outfile, ensure_ascii=False, cls=DateTimeEncoder)

    with open("files/channel_messages_new.json", encoding='utf-8') as file1, \
            open("files/channel_messages.json", encoding='utf-8') as file2:
        new = json.load(file1)
        old = json.load(file2)

    if len(old) > 2000:
        while len(old) > 1900:
            del old[-1]

    for i in range(len(new)):
        change_msg = []
        if new[i] not in old:
            for j in range(len(old)):
                if old[j]["id"] == new[i]["id"]:
                    old[j]["message"] = new[i]["message"]
                    change_msg.append(new[i])
                    break
            if new[i] not in change_msg:
                old.insert(0, new[i])
    for i in old:
        i["deleted"] = True
        for j in new:
            if i["id"] == j["id"]:
                i["deleted"] = False
                break

    for i in reversed(range(len(old))):
        for j in reversed(range(len(old))):
            coeff = fuzz.ratio(old[i]['message'], old[j]['message'])
            if 85 <= coeff and old[i]['id'] != old[j]['id']:
                if old[i]['date'] < old[j]['date']:
                    old.remove(old[i])
                    break

    old = [i for n, i in enumerate(old) if i not in old[n + 1:]]

    for msg_dict in old:
        for images in os.listdir(folder):
            end = images[-4:]
            name_img = images.removesuffix(end).split()
            data_msg = msg_dict['date'].translate(str.maketrans('', '', string.punctuation)).replace('T', '').replace(' ', '')

            if ("from_id" in msg_dict) and (msg_dict['from_id'] == int(name_img[0])) and (data_msg == name_img[1]):
                if ("media" in msg_dict) and (f'1323772109/{images}' not in msg_dict['media']):
                    msg_dict['media'].append(f'1323772109/{images}')
                else:
                    msg_dict['media'] = [f'1323772109/{images}']

    db_update(old, "message")
    stat_find()
    with open('files/channel_messages.json', 'w', encoding='utf8') as outfile:
        json.dump(old, outfile, ensure_ascii=False, cls=DateTimeEncoder)


def dump_all_media(chat_id):
    result = client(GetDialogsRequest(
        offset_date=None,
        offset_id=0,
        offset_peer=InputPeerEmpty(),
        limit=500,
        hash=0))

    for chat in result.chats:
        if chat.id == chat_id:
            messages = client.get_messages(chat, limit=100)
            for message in tqdm(messages):
                try:
                    path = str(chat_id) + '/' + str(message.from_id.user_id) \
                           + ' ' + str(message.date).translate(str.maketrans('', '', string.punctuation)).replace(' ',
                                                                                                                  '') \
                           + ' ' + str(message.id)
                    if not os.path.isfile(f'C:/Users/PoPo/PycharmProjects/Site_parser/euc/adsboard/static/adsboard/img/{path}.jpg') \
                            and not os.path.isfile(f'C:/Users/PoPo/PycharmProjects/Site_parser/euc/adsboard/static/adsboard/img/{path}.png') \
                            and not os.path.isfile(f'C:/Users/PoPo/PycharmProjects/Site_parser/euc/adsboard/static/adsboard/img/{path}.mp4'):
                        message.download_media(f'C:/Users/PoPo/PycharmProjects/Site_parser/euc/adsboard/static/adsboard/img/{path}')
                except:
                    continue
            break
    for file in os.listdir(folder):
        if file.endswith(".jpg") or file.endswith(".png"):
            try:
                foo = Image.open(f'{folder}/{file}')
                foo.save(f'{folder}/{file}', optimize=True, quality=40)
            except:
                continue


with open("files/cities.json", encoding='utf-8') as file1, open("files/firms.json", encoding='utf-8') as file2:
    cities = json.load(file1)
    firms = json.load(file2)

send_lst, triggers_city, triggers_send, trigger_run, trigger_cost, triggers_ad_type = \
    ['пересыл', 'отправлю', 'отправка', 'пересылом', 'отправкаврегионы', 'отправкапорегионам', 'пересылка', 'спересылом'], \
    ['старый', 'город', 'долго', 'городу', 'города', 'сток', 'городе', 'сколь', 'сколы', 'салат', 'стоек', 'суток'], \
    ['правка', 'безпересылки', 'безпересыла', 'пересел'],\
    ['мизерный', 'не', 'нет', 'минимальным', 'без', 'минимальный', 'почти', 'маленький', 'небольшой', 'чисто'], \
    ['цельная', 'пена', 'жена', 'цветная', 'камеру', 'б', 'у', 'одна', 'на', 'удалена', 'а', 'н', 'е', 'трубками',
     'договорная', 'труб', 'грубое', 'грубо', 'трубы', 'трубой', 'трубкой', 'трубки', 'зарубеж', 'шт', 'так',
     'затычкидлятрубs18'], ['подарю', 'продажи']


def fuz(dict, s_text, triggers=[]):
    val = '-'
    for key in dict:
        if key in s_text:
            val = dict.get(key)
            break
    if val == '-':
        for key in dict:
            n = process.extractOne(key, s_text)
            if 80 < n[1] and len(n[0]) > 4 and n[0] not in triggers:
                val = dict.get(key)
                break
    return val


def del_emoji(text, del_symbols):

    def replace_symbols(text_repl, symbols):
        for i in symbols:
            text_repl = text_repl.replace(i, ' ')
        return text_repl

    text = replace_symbols(text, del_symbols)

    regrex_pattern = re.compile(pattern = "["
        u"\U0001F600-\U0001F64F"  
        u"\U0001F300-\U0001F5FF"  
        u"\U0001F680-\U0001F6FF"  
        u"\U0001F1E0-\U0001F1FF"  
        "]+", flags = re.UNICODE)
    return regrex_pattern.sub(r'', text)


def run_find(msg, trigger=[]):
    run = None
    text = del_emoji(msg, '-,#.❗️₽🉐🧐🤔')
    text = text.lower().rstrip().replace('—', '').replace('≈', '').replace('•', '')\
        .translate(str.maketrans('', '', string.punctuation)).split()

    n = process.extractOne('пробег', text)
    d = n[0].translate(str.maketrans('', '', string.punctuation)).replace('—', '').replace('≈', '').replace('•', '')
    d = ''.join(j for j in d if not j.isalpha())

    if 80 < n[1] and len(n[0]) > 5:
        if d != '' and d.isdigit():
            run = int(d)
        else:
            idx = text.index(n[0])
            if ((len(text) > idx+1) or (len(text) > idx+4)) and text[idx+1] not in trigger and text[idx-1] not in trigger:
                d_next = text[idx + 1].replace('-', ' ').translate(str.maketrans('', '', string.punctuation))
                d_m = d_next
                d_next = ''.join(i for i in d_next if not i.isalpha())

                if d_next == '':
                    if len(text) > idx + 4:
                        for i in text[idx:idx + 5]:
                            if 'цена' in i:
                                run = None
                                break

                            idx2 = text.index(i) + 1
                            if (((len(i) <= 3 and ('k' in i or 'к' in i or 'т' in i)) or 'тыс' in i) and ('km' not in i and 'км' not in i))\
                                or (len(text) > idx2 and ((len(text[idx2]) <= 2 and ('k' in text[idx2] or 'к' in text[idx2])) or 'тыс' in text[idx2])
                                and ('km' not in text[idx2] and 'км' not in text[idx2])):

                                d_m = ''.join(j for j in i if not j.isalpha())
                                if d_m != '' and d_m.isdigit():
                                    run = int(d_m) * 10**3
                                    if run > 5*10**4:
                                        run = None
                                    break

                            d_m = ''.join(j for j in i if not j.isalpha() and 'ватт' not in i)
                            if d_m != '' and d_m.isdigit():
                                run = int(d_m)
                                if run >= 100:
                                    break
                            idx_i = text.index(i)
                            if d_m != '' and d_m.isdigit() and len(text) > idx_i + 1 and int(d_m) < 100:
                                d_next_2 = ''.join(i for i in text[idx_i + 1] if not i.isalpha())
                                if d_next_2 != '' and d_next_2.isdigit() and len(d_next_2) == 3:
                                    run = int(d_m + d_next_2)
                        if run is not None and 0 < run <= 5:
                            run = None

                    else:
                        for i in text[idx:]:
                            if 'цена' in i:
                                run = None
                                break
                            idx2 = text.index(i) + 1
                            if ((len(i) <= 3 and ('k' in i or 'к' in i or 'т' in i)) and ('km' not in i and 'км' not in i))\
                                or (len(text) > idx2 and ('k' == text[idx2] or 'к' == text[idx2])):

                                d_m = ''.join(j for j in i if not j.isalpha())
                                if d_m != '' and d_m.isdigit():
                                    run = int(d_m) * 10**3
                                    if run > 5*10**4:
                                        run = None
                                    break

                            d_m = ''.join(j for j in i if not j.isalpha())
                            if d_m != '' and d_m.isdigit():
                                run = int(d_m)
                                break

                    if run is None:
                        d_prev = text[idx - 1]
                        d_m = ''.join(j for j in d_prev if not j.isalpha() and 'ватт' not in d_prev)
                        if d_m != '' and d_m.isdigit():
                            run = int(d_m)

                        if text[idx - 1] == 'km' or text[idx - 1] == 'км':
                            d_m = ''.join(j for j in text[idx - 2] if not j.isalpha())
                            if d_m != '' and d_m.isdigit():
                                run = int(d_m)

                        if 'цена' not in text[idx - 3: idx] and 'ценник' not in text[idx - 3: idx]:
                            idx2 = text.index(d_prev) + 1
                            if ((len(d_prev) <= 3 and ('k' in d_prev or 'к' in d_prev or 'т' in d_prev[-1]))
                                and ('km' not in d_prev and 'км' not in d_prev and 'р' not in d_prev and 'wh' not in d_prev)) \
                                or (len(text) > idx2 and ('k' == text[idx2] or 'к' == text[idx2])):

                                d_m = ''.join(j for j in d_prev if not j.isalpha())
                                if d_m != '' and d_m.isdigit():
                                    run = int(d_m) * 10**3
                else:
                    if d_next.isdigit():
                        run = int(d_next)
                        if (((len(d_m) <= 3 and ('k' in d_m or 'к' in d_m or 'т' in d_m)) or ('тыс' in d_m)) and ('km' not in d_m and 'км' not in d_m))\
                            or (len(text) > idx + 2 and ((len(text[idx + 2]) <= 3 and ('k' in text[idx + 2] or 'к' in text[idx + 2]
                            or 'т' in text[idx + 2])) or ('тыс' in text[idx + 2])) and ('km' not in text[idx + 2] and 'км' not in text[idx + 2])):

                            if (run * 10**3) < 5*10**4:
                                run = run * 10**3
                            else:
                                if 'км' in text[idx - 1] and 'ч' not in text[idx - 1]:
                                    d_m = ''.join(j for j in text[idx - 1] if not j.isalpha())
                                    if d_m != '' and d_m.isdigit():
                                        run = int(d_m)
                        else:
                            if len(text) > idx + 3:
                                d_next_2 = ''.join(i for i in text[idx + 2] if not i.isalpha())

                                if d_next_2 != '' and d_next_2.isdigit() and int(d_next) != 0 and int(d_next_2) < 1000\
                                        and len(text[idx + 2]) <= 3 and len(text[idx + 3]) <=3 and ('к' in text[idx + 3]
                                        or 'т' in text[idx + 3]) and ('км' not in d_m and 'км' not in text[idx + 3]
                                        and 'р' not in d_m and 'р' not in text[idx + 3]):
                                        run = int(d_next + d_next_2)*100

                            if len(text) > idx + 2:
                                d_next_2 = ''.join(i for i in text[idx + 2] if not i.isalpha())
                                if 0 < run < 20 and d_next_2 != '' and d_next_2.isdigit() and len(d_next_2)==3:
                                    run = int(d_next + d_next_2)
            if n[0] == 'пробега' and text[idx - 1] == 'без':
                run = 0
    return run


def cost_find(msg, trigger=[]):
    cost = None
    text = del_emoji(msg, '|-,#.+/()«»—❗️₽🉐🧐🤔✨=')
    text = text.lower().rstrip().translate(str.maketrans('', '', string.punctuation)).split()
    for word in trigger:
        if word in text:
            text.remove(word)
    n = process.extractOne('цена', text)
    d = n[0].translate(str.maketrans('', '', string.punctuation))
    d = ''.join(j for j in d if not j.isalpha())

    if 75 <= n[1] and n[0] not in trigger:
        if d != '' and d.isdigit():
            cost = int(d)
            if 'к' in n[0] or 'т' in n[0] or 'k' in n[0]:
                cost = int(d) * 1000
        else:
            idx = text.index(n[0])
            if (len(text) > idx + 1) and text[idx] not in trigger:
                d_next = text[idx+1].translate(str.maketrans('', '', string.punctuation))
                d_m = d_next
                d_next = ''.join(i for i in d_next if not i.isalpha())
                if d_next == '':
                    if len(text) > idx + 4:
                        for word in text[idx:idx + 5]:
                            d_m = ''.join(j for j in word if not j.isalpha() and 'ватт' not in word)
                            if d_m != '' and d_m.isdigit():
                                cost = int(d_m)

                                if 'к' in word or 'т' in word:
                                    cost = int(d_m) * 10**3

                                word_idx = text.index(word)

                                if len(text) > word_idx + 1:
                                    d_next_2 = text[word_idx + 1]
                                    if (len(d_next_2) <= 3) and ('к' in d_next_2 or 'т' in d_next_2 or 'k' in d_next_2) and (
                                            int(d_m) < 10**3):
                                        cost = int(d_m) * 10**3
                                if len(text) > word_idx + 1:
                                    d_next_2 = ''.join(i for i in text[word_idx + 1] if not i.isalpha())
                                    if d_next_2 != '' and d_next_2.isdigit() and int(d_next_2) < 10**3 and len(d_next_2)==3:
                                        cost = int(d_m + d_next_2)
                                break

                    if type(cost) == int and cost < 10:
                        cost = None

                    k = 0
                    if cost is None:
                        for word in text[idx:]:
                            k += 1
                            if k == 7:
                                break
                            d_m = ''.join(j for j in word if not j.isalpha() and 'ватт' not in word)
                            if d_m != '' and d_m.isdigit() and 10 < int(d_m) < 10**6 and ('км' not in word):
                                cost = int(d_m)

                                if ('к' in word or 'k' in word or 'т' in word) and ('км' not in word) and (int(d_m)<10**3):
                                    cost = int(d_m) * 1000
                                    break

                                if len(text) > text.index(word) + 1:
                                    word_next = text[text.index(word) + 1]
                                    if (('к' in word or 'k' in word or 'т' in word) and ('км' not in word)) or\
                                        (len(word_next) <= 3 and ('к' in word_next or 'k' in word_next or 'т' in word_next) and ('км' not in word_next)):
                                        cost = int(d_m) * 1000
                                        break

                                word_idx = text.index(word)
                                if len(text) > word_idx + 1:
                                    d_next_2 = ''.join(i for i in text[word_idx+1] if not i.isalpha())
                                    if d_next_2 != '' and d_next_2.isdigit() and int(d_next_2) < 1000:
                                        cost = int(d_m + d_next_2)
                                        break

                        if type(cost) == int and cost <= 20:
                            cost = None

                else:
                    if d_next.isdigit():
                        cost = int(d_next)

                    if ('к' in d_m or 'т' in d_m or 'k' in d_m) and (d_next.isdigit()) and len(d_next) < 10**6 and int(d_next)< 10**3:
                        cost = int(d_next)*1000

                    if len(text) > idx + 2:
                        d_next_2 = text[idx + 2]
                        if (((len(d_next_2)<=3) and ('к' in d_next_2 or 'т' in d_next_2 or 'k' in d_next_2)
                             and d_next_2 not in trigger) or ('тысяч' in d_next_2)) and (int(d_next) < 1000):
                            cost = int(d_next) * 1000

                    if (len(text) > idx+2) and (cost < 10**4):
                        d_next_2 = ''.join(i for i in text[idx + 2] if not i.isalpha())
                        if d_next_2 != '' and (d_next.isdigit() and d_next_2.isdigit()) and (int(d_next) < 1000 and int(d_next_2) < 1000) :
                            cost = int(d_next + d_next_2)
                            if 'к' in text[idx + 2]:
                                cost *= 100

    if cost is not None and cost > 9 * 10**6:
        cost = None
    if cost is None:

        n = process.extractOne('руб', text)
        d = n[0].translate(str.maketrans('', '', string.punctuation))
        d = ''.join(j for j in d if not j.isalpha())

        if 80 < n[1] and n[0] not in trigger and (len(n[0])>1 or n[0] == 'р'):
            if d != '':
                cost = int(d)
                if cost == 0:
                    cost = None
            elif d == '' or cost == 0:
                idx = text.index(n[0])
                d_prev = text[idx - 1]
                d_m = ''.join(j for j in d_prev if not j.isalpha() and 'ватт' not in d_prev)
                if d_m != '' and d_m.isdigit():
                    cost = int(d_m)

                    if (('к' in d_prev or 'т' in d_prev or 'k' in d_prev) and len(d_m) < 10**6 and int(d_m) < 10**3)\
                            or 'килорублей' in n[0]:
                        cost = int(d_m)*1000

                        if cost == 0:
                            cost = None

                    d_prev_2 = text[idx - 2]
                    d_prev_2 = ''.join(j for j in d_prev_2 if not j.isalpha() and 'ватт' not in d_prev_2)
                    if d_prev_2 != '' and d_prev_2.isdigit() and len(d_prev_2) < 4 and (len(d_m) == 3 and int(d_m) < 10**3):

                        if int(d_m) != 0:
                            if (('-'+d_m) in msg or ('- '+d_m) in msg) or \
                                    (('-'+d_prev_2) in msg or ('- '+d_prev_2) in msg or ('.'+d_prev_2) in msg):
                                cost = int(d_m)
                            else:
                                cost = int(d_prev_2 + d_m)

                        else:
                            cost = int(d_prev_2 + d_m)

                if cost is None:
                    if (('к' in d_prev or 'т' in d_prev or 'k' in d_prev) and len(d_prev)<=3) or d_prev == 'тыс':
                        d_prev_3 = text[idx - 2]
                        d_prev_3 = ''.join(j for j in d_prev_3 if not j.isalpha() and 'ватт' not in d_prev_3)
                        if d_prev_3 != '' and d_prev_3.isdigit():
                            if int(d_prev_3) <= 999:
                                cost = int(d_prev_3)*10**3
                            else:
                                cost = int(d_prev_3)
    if cost is None:

        last_str = msg.split()[-1]
        last = ''.join(j for j in text[-1] if not j.isalpha() and 'ватт' not in text[-1])

        if (last_str.isdigit() or (('р' in last_str) or ('₽' in last_str))) and last != '' and last.isdigit() and (
                100 < int(last) < 10 ** 6 or int(last) == 0):

            if int(last) >= 1000:
                cost = int(last)
            elif 0 < int(last) < 1000:  # 9028 17276
                cost = int(last)

            if (len(last_str) <= 5) and ('к' in last_str or 'т' in last_str) and (
                    'км' not in last_str and 'вт' not in last_str and 'шт' not in last_str):  # втч
                cost = int(last) * 1000

            d_prev_2 = text[-2]
            d_prev_2 = ''.join(j for j in d_prev_2 if not j.isalpha() and 'ватт' not in d_prev_2)

            if d_prev_2 != '' and d_prev_2.isdigit() and len(d_prev_2) < 4 and (len(last) == 3 and int(last) == 0):
                cost = int(d_prev_2 + last)

    return cost


def ad_type_find(msg, triggers):

    key_sale, key_buy = ['продам', 'продаю', 'продажа'], ['куплю', 'купить']
    ad_type = ''
    text = msg.lower().rstrip().replace('—', '').replace('≈', '').replace('•', '') \
        .translate(str.maketrans('', '', string.punctuation)).split()

    for key in key_sale:
        if key in text:
            ad_type = 'sale'
            break
    for key in key_buy:
        if key in text:
            ad_type = 'buy'
            break

    if ad_type == '':
        f = False
        for key in key_sale:
            n = process.extractOne(key, text)
            if 80 < n[1] and len(n[0]) > 4 and n[0] not in triggers:
                ad_type = 'sale'
                f = True
                break
        for key in key_buy:
            n = process.extractOne(key, text)
            if 80 < n[1] and len(n[0]) > 4 and n[0] not in triggers:
                ad_type = 'buy'
                f = True
                break
        if not f:
            ad_type = 'other'

    return ad_type


def db_update(file, table):
    con = pymysql.connect(host="localhost", user="root", password="elsi1979", db="parser")
    cursor = con.cursor()

    if table == "user":
        for item in file:
            id = item.get("id")
            first_name = item.get("first_name")
            last_name = item.get("last_name")
            nickname = item.get("user")
            phone = item.get("phone")
            cursor.execute(f"INSERT INTO parser.{table}"
                           "(user_id, first_name, last_name, nickname, phone) VALUES (%s,%s,%s,%s,%s)"
                           "ON DUPLICATE KEY UPDATE user_id=%s, first_name=%s, last_name=%s, nickname=%s, phone=%s",
                           (id, first_name, last_name, nickname, phone,
                            id, first_name, last_name, nickname, phone))
        con.commit()
        con.close()
    else:
        for item in file:
            try:
                id = item.get("id")
                link = item.get("link")
                timedate = item.get("date")
                timedate = timedate.replace("T", " ")[:-6]
                deleted = item.get("deleted")

                message = item.get("message")

                text = message.lower().replace('#', ' ').replace('.', ' ').replace(',', ' ')
                split_text = text.split()

                send = False
                for key in send_lst:
                    if key in split_text:
                        send = True
                        break
                if not send:
                    for key in send_lst:
                        n = process.extractOne(key, split_text)
                        if 80 < n[1] and len(n[0]) > 4 and n[0] not in triggers_send:
                            idx = split_text.index(n[0])
                            if split_text[idx - 1] == 'без' or split_text[idx - 1] == 'нет' or\
                               split_text[idx - 1] == 'не' or (len(split_text) > idx+1 and split_text[idx+1] == 'нет'):
                                send = False
                                break
                            else:
                                send = True
                                break

                city = fuz(cities, split_text, triggers_city)

                firm = fuz(firms, split_text)

                run = run_find(message, trigger_run)

                cost = cost_find(message, trigger_cost)

                ad_type = ad_type_find(message, triggers_ad_type)

                from_id = item.get("from_id")

                media = item.get("media")
                if media is None:
                    media = '-'
                else:
                    media = ", ".join(media)

                cursor.execute(
                    f"INSERT INTO parser.{table}"
                    "(msg_id, link, timedate, deleted, message, ad_type, send, city, firm, run, cost, from_id, media) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    "ON DUPLICATE KEY UPDATE deleted=%s, message=%s, ad_type=%s,send=%s, city=%s, firm=%s, run=%s, cost=%s, media=%s",
                    (id, link, timedate, deleted, message, ad_type, send, city, firm, run, cost, from_id, media,
                     deleted, message, ad_type, send, city, firm, run, cost, media))
            except Exception as e:
                print(e)
                print(item)
        con.commit()
        con.close()


def stat_find():

    details = ['корпус', 'батарея', 'двигатель', 'контроллер', 'покрышка', 'камера', 'амортизатор', 'bms', 'бампер',
               'крышка', 'накладка', 'блок','динамики', 'педали', 'аккумулятор', 'брызговик', 'ручка', 'шина', 'чехол',
               'мотор', 'покрышка', 'шлем', 'амортизатор', 'подушка', 'фара', 'аккум', 'стойки', 'корпус', 'упоры', 'батка',
               'сиденье', 'зарядка', 'динамики', 'обод', 'подставка']

    cost_all, c_all, cost_year, c_year, cost_month, c_month, c_details = 0, 0, 0, 0, 0, 0, 0
    current_date = str(date.today()).split('-')

    con = pymysql.connect(host="localhost", user="root", password="elsi1979", db="parser")
    cursor = con.cursor()

    select_date = "SELECT timedate, cost FROM parser.message"
    select_all_msg = "SELECT COUNT(*) FROM parser.message"
    select_year_msg = f"SELECT count(*) FROM parser.message WHERE timedate LIKE '{current_date[0]}%'"
    select_month_msg = f"SELECT count(*) FROM parser.message WHERE timedate LIKE '{current_date[0]+'-'+current_date[1]}%'"
    select_euc = "SELECT count(*) FROM parser.sale WHERE cost >= 50000 and deleted != 1"
    select_detail = "SELECT message, cost FROM parser.sale WHERE cost < 50000 and deleted != 1"

    f_s = {}
    with con.cursor() as cursor:

        firm = ["Begode", "Veteran Sherman", "Gotway", "KingSong", "InMotion",
                "Ninebot", "Rockwheel", "Airwheel", "Solowheel", "Madbull"]

        for i in firm:
            select_firm_sum = f"SELECT sum(cost) FROM parser.message WHERE firm = '{i}' and cost > 0"
            cursor.execute(select_firm_sum)
            select_firm_sum = cursor.fetchall()

            select_firm_count = f"SELECT count(cost) FROM parser.message WHERE firm = '{i}' and cost > 0"
            cursor.execute(select_firm_count)
            select_firm_count = cursor.fetchall()

            f_s[i] = round(select_firm_sum[0][0] / select_firm_count[0][0])

        for i, j in f_s.items():
            cursor.execute(f"UPDATE parser.stat SET {i.lower().replace(' ', '_')}=%s LIMIT 1", (j))

        cursor.execute(select_date)
        result = cursor.fetchall()
        for row in result:

            date_msg = str(row[0]).split('-')
            cost = row[1]

            if cost is not None:
                cost_all += cost
                c_all += 1

                if date_msg[0] == current_date[0]:
                    cost_year += cost
                    c_year += 1

                if date_msg[1] == current_date[1]:
                    cost_month += cost
                    c_month += 1

        mid_cost_all = round(cost_all / c_all)
        mid_cost_year = round(cost_year / c_year)
        mid_cost_month = round(cost_month / c_month)

        cursor.execute(select_all_msg)
        select_all_msg = cursor.fetchall()
        count_all = select_all_msg[0][0]

        cursor.execute(select_year_msg)
        select_year_msg = cursor.fetchall()
        count_year = select_year_msg[0][0]

        cursor.execute(select_month_msg)
        select_month_msg = cursor.fetchall()
        count_month = select_month_msg[0][0]

        cursor.execute(select_euc)
        select_euc = cursor.fetchall()[0][0]

        cursor.execute(select_detail)
        select_detail = cursor.fetchall()

        for obj in select_detail:
            msg = del_emoji(obj[0], '|-,#.+/()«»—❗️₽🉐🧐🤔✨=')
            msg = msg.lower().rstrip().translate(str.maketrans('', '', string.punctuation)).split()

            if ('моноколесо' in msg or 'колесо' in msg) and (obj[1] >= 10**4):
                select_euc += 1
            else:
                for detail in details:
                    n = process.extractOne(detail, msg)

                    if 80 <= n[1] and len(n[0]) > 2 and 'моноколесо' not in msg and 'колесо' not in msg:
                        if n[0] in msg:
                            c_details += 1
                            break

        count_EUC = select_euc
        count_details = c_details
        cursor.execute("""UPDATE parser.stat SET mid_cost_all=%s, mid_cost_year=%s, mid_cost_month=%s, count_all=%s,
                                  count_year=%s, count_month=%s, count_EUC=%s, count_details=%s LIMIT 1""",
                       (mid_cost_all, mid_cost_year, mid_cost_month, count_all, count_year, count_month, count_EUC,
                        count_details))

    con.commit()
    con.close()


dump_all_media(from_chat_id)


async def main():
    channel = await client.get_entity(link)
    await dump_all_participants(channel)
    await dump_all_messages(channel)


with client:
    client.loop.run_until_complete(main())

while True:
    time.sleep(1)
    z = time.localtime()
    if z.tm_hour == 12 and z.tm_min == 0 and z.tm_sec == 0:
        with client:
            client.loop.run_until_complete(main())
            if client.loop.is_closed():
                client.loop.close()