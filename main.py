import hashlib
import hmac
import urllib
import time
import numpy as np
import pandas
import requests
from threading import Thread
from Queue import Queue
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)


def build_indico_request(path, params, api_key=None, secret_key=None, only_public=False, persistent=False):
    items = params.items() if hasattr(params, 'items') else list(params)
    if api_key:
        items.append(('ak', api_key))
    if only_public:
        items.append(('onlypublic', 'yes'))
    if secret_key:
        if not persistent:
            items.append(('timestamp', str(int(time.time()))))
        items = sorted(items, key=lambda x: x[0].lower())
        url = '%s?%s' % (path, urllib.urlencode(items))
        signature = hmac.new(secret_key, url, hashlib.sha1).hexdigest()
        items.append(('signature', signature))
    if not items:
        return path
    return '%s?%s' % (path, urllib.urlencode(items))


def copy_dict(d, *keys):
    """Make a copy of only the `keys` from dictionary `d`."""
    return {key: d[key] for key in keys}


def get_info_meeting(id_event):
    url = BASE_URL + build_indico_request('/export/event/%s.json' % id_event, {'detail': 'contributions'}, API_KEY, SECRET_KEY)
    r = requests.get(url)
    data_event = r.json()['results'][0]
    data_contributions = data_event['contributions']

    result_meeting = {"id_meeting": [data_event['id']],
                      "contributions": [len(data_contributions)],
                      "title": [data_event['title']],
                      "date_meeting": [np.datetime64(data_event['startDate']['date'])]}
    result_contributions = {'id_meeting': [], 'duration': [], 'title': [],
                            'speaker': [], 'id_contribution': []}

    for dc in data_contributions:
        if len(dc['speakers']) > 0:
            result_contributions['speaker'].append(dc['speakers'][0]['fullName'])
        else:
            result_contributions['speaker'].append('NO SPEAKER')
        result_contributions['duration'].append(dc['duration'])
        result_contributions['title'].append(dc['title'])
        result_contributions['id_contribution'].append(data_event['id'])
        result_contributions['id_meeting'].append(data_event['id'])

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
            self.message_queue.put('done')
            self.queue.task_done()


def reporter(queue, total):
    i = 0
    while True:
        message = queue.get()
        if 'done' in message:
            i += 1
            print i, total
        else:
            print message
        queue.task_done()


def main(start, stop, API_KEY, SECRET_KEY, category, meeting_title, BASE_URL="https://indico.cern.ch"):

    url = BASE_URL + build_indico_request('/export/categ/%s.json' % category,
                                          {'from': start, 'to': stop},
                                          API_KEY, SECRET_KEY)

    logging.info("query all event in category %s", category)
    r = requests.get(url)
    if not r.status_code == requests.codes.ok:
        raise IOError("cannot connect")

    queue = Queue()
    out_queue = Queue()
    message_queue = Queue()

    logging.info("creating threads")
    for i in xrange(50):
        t = ThreadMeeting(queue, out_queue, message_queue)
        t.setDaemon(True)
        t.start()

    data_categories = r.json()
    logging.info("found %d events in category" % len(data_categories['results']))

    table_meetings = None
    table_contributions = None

    nevents = 0
    logging.info("populating queues")
    for d in data_categories['results']:
        if meeting_title.lower() in d['title'].lower():
            nevents += 1
            id_event = d['id']
            queue.put(id_event)
    logging.info("%d events in queue" % nevents)

    r = Thread(target=reporter, args=(message_queue, nevents))
    r.setDaemon(True)
    r.start()

    queue.join()
    message_queue.join()

    logging.info('merging %d output', len(out_queue.queue))
    for result_meeting, result_contributions in out_queue.queue:
        result_meeting['my_title'] = [meeting_title]
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

    pd_meeting = pd_meeting.set_index('id_meeting')
    pd_contribution = pd_contribution.set_index('id_contribution')

    logging.info("writing")
    pd_meeting.to_pickle('%s_meeting.pkl' % meeting_title)
    pd_contribution.to_pickle('%s_contributions.pkl' % meeting_title)
    logging.info("done")

if __name__ == "__main__":
    from config import API_KEY, SECRET_KEY, BASE_URL
    start = '2010-01-01'
    to = '2015-05-31'
    meeting_title = 'egamma calibration'
    meeting_title = 'Photon ID'
    meeting_title = 'T&P'
    meeting_title = 'Egamma meeting'
    #category = '490'

    meeting_title = "HSG1"
    category = '6139'

    main(start, to, API_KEY, SECRET_KEY, category, meeting_title, BASE_URL)
