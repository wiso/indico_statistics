#!/usr/bin/env python

import datetime
import logging
import urllib.error
import urllib.parse
import urllib.request
from argparse import ArgumentParser
from collections import Counter

import requests

from config import BASE_URL, TOKEN

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s %(asctime)s %(threadName)s: %(message)s"
)
logging.getLogger("requests").setLevel(logging.WARNING)


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
        raise IOError("not valid response from %s:\n  %s " % (r.url, r.json()))
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


def get_all_meeting_from_category(category_id, start, stop):
    start_date_str = start.strftime("%Y-%m-%d")
    end_date_str = stop.strftime("%Y-%m-%d")
    request_url = f"export/categ/{category_id}.json"
    request_params = {"from": start_date_str, "to": end_date_str}
    response = create_request(BASE_URL, request_url, request_params, TOKEN)

    logging.info("Querying all events in category %s", category_id)
    logging.debug("Request URL: %s", response.url)

    if response.status_code != requests.codes.ok:
        raise IOError(f"Cannot connect to {response.url}. Status code: {response.status_code}")
    logging.info("Query successful")
    return response.json()


parser = ArgumentParser(description="Retrieve speakers from the Indico event management system.")
parser.add_argument("--start", required=True, help="Start date in the format DD-MM-YYYY.")
parser.add_argument("--stop", required=True, help="End date in the format DD-MM-YYYY.")
parser.add_argument("--category", help="Category ID to filter events by. Default is 490.", default="490")
args = parser.parse_args()

try:
    start = datetime.datetime.strptime(args.start, "%d-%m-%Y")
    stop = datetime.datetime.strptime(args.stop, "%d-%m-%Y")
except ValueError:
    print("Invalid date format. Please use the format YYYY-MM-DD.")
    raise


counter = Counter()

all_meetings = get_all_meeting_from_category(args.category, start, stop)
all_meetings = all_meetings["results"]
for i, meeting in enumerate(all_meetings, 1):
    speakers = get_speakers_event(meeting["id"])
    num_speakers = len(speakers)
    logging.info(
        "Retrieved %d speakers for meeting '%s' from %s (%d/%d)",
        num_speakers,
        meeting["title"],
        meeting["startDate"],
        i,
        len(all_meetings),
    )
    meeting_title = meeting["title"].lower()
    for speaker in speakers:
        counter[(speaker, meeting_title)] += 1

max_speaker_len = max(len(speaker) for speaker, _ in counter.keys())
max_title_len = max(len(title) for _, title in counter.keys())

for (speaker, title), count in counter.items():
    print(f"{speaker:{max_speaker_len}} {title:{max_title_len}} {count:>3}")
