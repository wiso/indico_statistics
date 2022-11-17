import requests
import urllib.request, urllib.parse, urllib.error
import logging
from collections import Counter

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s %(asctime)s %(threadName)s: %(message)s"
)
logging.getLogger("requests").setLevel(logging.WARNING)
from config import TOKEN, BASE_URL


class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


def create_request(BASE_URL, query, params, token):
    url = urllib.parse.urljoin(BASE_URL, query)
    r = requests.get(url, params=params, auth=BearerAuth(token))
    if not r.status_code == requests.codes.ok:
        raise IOError("cannot connect to %s" % r.url)

    return r


def get_speakers_event(event):
    r = create_request(BASE_URL, "export/event/%s.json" % event, {"detail": "contributions"}, TOKEN)

    json = r.json()
    all_speakers_fullName = []

    for contribution in json["results"][0]["contributions"]:
        speakers = contribution["speakers"]
        for speaker in speakers:
            all_speakers_fullName.append(speaker["fullName"])
    return all_speakers_fullName


def get_all_meeting_from_category(id_category, start, stop):
    r = create_request(
        BASE_URL, "export/categ/%s.json" % id_category, {"from": start, "to": stop}, TOKEN
    )

    logging.info("query all event in category %s", id_category)
    logging.debug("request = %s", r.url)

    if not r.status_code == requests.codes.ok:
        raise IOError("cannot connect to %s" % r.url)
    logging.info("query category good")
    return r.json()


start = "2022-05-01"
stop = "2022-10-31"

counter = Counter()

all_meetings = get_all_meeting_from_category("490", start, stop)
all_meetings = all_meetings["results"]
for i, meeting in enumerate(all_meetings, 1):
    speakers = get_speakers_event(meeting["id"])
    logging.info(
        "getting speakers for meeting %s from %s %d/%d",
        meeting["title"],
        meeting["startDate"],
        i,
        len(all_meetings),
    )
    meeting_title = meeting["title"].lower()
    for speaker in speakers:
        counter[(meeting_title, speaker)] += 1

for k, v in counter.items():
    print(f"{k[0]} {k[1]}: {v}")
