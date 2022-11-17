import urllib.request, urllib.parse, urllib.error
import re
import numpy as np
import pandas
import requests
from threading import Thread
from queue import Queue
from memoizer import memoized
from config import BASE_URL, TOKEN
import logging

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
        raise IOError("cannot connect to %s" % r.url)

    return r


def copy_dict(d, *keys):
    """Make a copy of only the `keys` from dictionary `d`."""
    return {key: d[key] for key in keys}


@memoized
def get_all_meeting_from_category(id_category, start, stop):
    r = create_request(
        BASE_URL, "export/categ/%s.json" % id_category, {"from": start, "to": stop}, TOKEN
    )

    logging.info("query all event in category %s", id_category)
    logging.debug("request = %s", r.url)
    logging.debug("query category good")
    return r.json()


def get_info_meeting(id_event):
    r = create_request(BASE_URL, "export/event/%s.json" % id_event, {"detail": "contributions"}, TOKEN)

    data_event = r.json()["results"][0]
    data_contributions = data_event["contributions"]

    result_meeting = {
        "id_meeting": [data_event["id"]],
        "contributions": [len(data_contributions)],
        "title": [data_event["title"]],
        "date_meeting": [np.datetime64(data_event["startDate"]["date"])],
    }
    result_contributions = {
        "id_meeting": [],
        "duration": [],
        "title": [],
        "speaker": [],
        "id_contribution": [],
    }

    for dc in data_contributions:
        if len(dc["speakers"]) > 0:
            result_contributions["speaker"].append(dc["speakers"][0]["fullName"])
        else:
            result_contributions["speaker"].append("NO SPEAKER")
        result_contributions["duration"].append(dc["duration"])
        result_contributions["title"].append(dc["title"])
        result_contributions["id_contribution"].append(dc["id"])
        result_contributions["id_meeting"].append(data_event["id"])

    return result_meeting, result_contributions


class ThreadMeeting(Thread):
    def __init__(self, queue, out_queue, message_queue):
        Thread.__init__(self)
        self.queue = queue
        self.out_queue = out_queue
        self.message_queue = message_queue

    def run(self):
        while True:
            id_event = self.queue.get()
            self.out_queue.put(get_info_meeting(id_event))
            self.message_queue.put("done")
            self.queue.task_done()


def reporter(queue, total):
    i = 0
    while True:
        message = queue.get()
        if "done" in message:
            i += 1
            logging.info("%s, %s", i, total)
        else:
            logging.info("%s", message)
        queue.task_done()


def job(
    start,
    stop,
    category,
    meeting_title,
    output_file,
):
    data_categories = get_all_meeting_from_category(category, start, stop)

    queue = Queue()
    out_queue = Queue()
    message_queue = Queue()

    logging.info("creating threads")
    for i in range(50):
        t = ThreadMeeting(queue, out_queue, message_queue)
        t.name = "thread-category-%s-%d" % (category, i)
        t.daemon = True
        t.start()

    logging.info("found %d events in category", len(data_categories["results"]))
    if len(data_categories["results"]) == 0:
        logging.warning("no event in category %s", category)

    table_meetings = None
    table_contributions = None

    nevents = 0
    logging.info("populating queues")
    for d in data_categories["results"]:
        if meeting_title.search(d["title"]):
            nevents += 1
            id_event = d["id"]
            queue.put(id_event)
        else:
            logging.info('"%s" id=%s cannot match regex %s',  d["title"], d["id"], meeting_title)

    logging.info("%d events in queue", nevents)

    r = Thread(target=reporter, args=(message_queue, nevents), name="thread-reporter")
    r.daemon = True
    r.start()

    queue.join()
    message_queue.join()

    logging.info("merging %d output", len(out_queue.queue))
    for result_meeting, result_contributions in out_queue.queue:
        result_meeting["my_title"] = [output_file]
        if table_meetings is None:
            table_meetings = result_meeting
        else:
            for k in table_meetings:
                table_meetings[k] += result_meeting[k]
        if table_contributions is None:
            table_contributions = result_contributions
        else:
            for k in result_contributions:
                table_contributions[k] += result_contributions[k]

    logging.info("creating pandas objets")
    pd_meeting = pandas.DataFrame.from_dict(table_meetings)
    pd_contribution = pandas.DataFrame.from_dict(table_contributions)

    if len(pd_meeting) == 0:
        logging.info("no events found")
        return

    pd_meeting = pd_meeting.set_index("id_meeting")
    pd_contribution = pd_contribution.set_index("id_contribution")

    logging.info("writing")
    pd_meeting.to_pickle("%s_meeting.pkl" % output_file)
    pd_contribution.to_pickle("%s_contributions.pkl" % output_file)
    logging.info("done")


if __name__ == "__main__":

    start = "2014-01-01"
    import datetime

    to = datetime.datetime.now().strftime("%Y-%m-%d")

    inputs = (
        ("490", re.compile("egamma calibration", re.IGNORECASE), "calibration"),
        ("490", re.compile("Photon ID", re.IGNORECASE), "photon_id"),
        ("490", re.compile("Electron identification|Egamma T&P", re.IGNORECASE), "electron_id"),
        ("490", re.compile("T&P software", re.IGNORECASE), "tp_software"),
        ("490", re.compile("Informal ML", re.IGNORECASE), "informal_ml"),
        ("490", re.compile("Egamma meeting", re.IGNORECASE), "egamma"),
        # ('6139', re.compile('HSG1', re.IGNORECASE), 'HSG1'),
        # ('6139', re.compile('HSG3', re.IGNORECASE), 'HSG3'),
        # ("6142", re.compile("HGam Coupling meeting", re.IGNORECASE), "HGam_coupling"),
        # ("6142", re.compile("HGam Fiducial", re.IGNORECASE), "HGam_xsection"),
        # ("6142", re.compile("VBF H", re.IGNORECASE), "HGam_VBF"),
        # ("6142", re.compile("Hyy+MET", re.IGNORECASE), "HGam_MET"),
        # ("6142", re.compile("Zgamma", re.IGNORECASE), "HGam_Zgamma"),
        # ("6142", re.compile("Mass", re.IGNORECASE), "HGam_mass"),
        # ("6142", re.compile("ttH", re.IGNORECASE), "HGam_ttH"),
        # ("6142", re.compile("Hyy\+MET", re.IGNORECASE), "HGam_yyMET"),
        # ("6142", re.compile("^HGamma$|HGam sub-group meeting", re.IGNORECASE), "HGam_plenary"),
        # (
        #    "6142",
        #    re.compile("High-mass diphotons|High-Low-mass diphoton", re.IGNORECASE),
        #    "HGam_yysearch",
        # ),
        #        ('4162', re.compile('WWgamgam', re.IGNORECASE), 'HGam_yyWW'),
    )

    threads = []
    for input in inputs:
        t = Thread(
            target=job,
            args=(start, to, input[0], input[1], input[2]),
            name="thread-meeting-%s" % input[2],
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
