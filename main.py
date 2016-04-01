import hashlib
import hmac
import urllib
import re
import time
import numpy as np
import pandas
import requests
from threading import Thread
from Queue import Queue
from memoizer import memoized
from config import API_KEY, SECRET_KEY, BASE_URL
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(asctime)s %(threadName)s: %(message)s')
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


@memoized
def get_all_meeting_from_category(id_category, start, stop):
    url = BASE_URL + build_indico_request('/export/categ/%s.json' % id_category,
                                          {'from': start, 'to': stop},
                                          API_KEY, SECRET_KEY)

    logging.info("query all event in category %s", id_category)
    r = requests.get(url)
    if not r.status_code == requests.codes.ok:
        raise IOError("cannot connect to %s" % url)
    logging.info("query category good")
    return r


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
            logging.info('%s, %s', i, total)
        else:
            logging.info('%s', message)
        queue.task_done()


def job(start, stop, API_KEY, SECRET_KEY, category, meeting_title, output_file, BASE_URL="https://indico.cern.ch"):
    r = get_all_meeting_from_category(category, start, stop)

    queue = Queue()
    out_queue = Queue()
    message_queue = Queue()

    logging.info("creating threads")
    for i in xrange(50):
        t = ThreadMeeting(queue, out_queue, message_queue)
        t.setName('thread-category-%s-%d' % (category, i))
        t.setDaemon(True)
        t.start()

    data_categories = r.json()
    logging.info("found %d events in category" % len(data_categories['results']))
    if len(data_categories['results']) == 0:
        logging.warning("no event in category %d" % category)

    table_meetings = None
    table_contributions = None

    nevents = 0
    logging.info("populating queues")
    for d in data_categories['results']:
        if meeting_title.search(d['title']):
            nevents += 1
            id_event = d['id']
            queue.put(id_event)
    logging.info("%d events in queue" % nevents)

    r = Thread(target=reporter, args=(message_queue, nevents), name='thread-reporter')
    r.setDaemon(True)
    r.start()

    queue.join()
    message_queue.join()

    logging.info('merging %d output', len(out_queue.queue))
    for result_meeting, result_contributions in out_queue.queue:
        result_meeting['my_title'] = [output_file]
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

    pd_meeting = pd_meeting.set_index('id_meeting')
    pd_contribution = pd_contribution.set_index('id_contribution')

    logging.info("writing")
    pd_meeting.to_pickle('%s_meeting.pkl' % output_file)
    pd_contribution.to_pickle('%s_contributions.pkl' % output_file)
    logging.info("done")

if __name__ == "__main__":

    start = '2014-01-01'
    import datetime
    to = datetime.datetime.now().strftime("%Y-%m-%d")

    inputs = (('490', re.compile('egamma calibration', re.IGNORECASE), 'egamma_calibration'),
              ('490', re.compile('Photon ID', re.IGNORECASE), 'photon_id'),
              ('490', re.compile('T&P', re.IGNORECASE), 'tp'),
              ('490', re.compile('Egamma meeting', re.IGNORECASE), 'egamma'),
              ('6139', re.compile('HSG1', re.IGNORECASE), 'HSG1'),
              ('6139', re.compile('HSG3', re.IGNORECASE), 'HSG3'))

    threads = []
    for input in inputs:
        t = Thread(target=job, args=(start, to, API_KEY, SECRET_KEY, input[0], input[1], input[2], BASE_URL),
                   name='thread-meeting-%s' % input[2])
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
