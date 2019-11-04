import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import requests
from bs4 import BeautifulSoup
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import slack

client = slack.WebClient(token='<Slackbot-token-here>')

url = 'https://www.remitly.com/ca/en/india/pricing'

# URL for FOREX - https://www.bankofcanada.ca/valet/observations/FXCADINR/json?recent=1

# Initialize Firebase
cred = credentials.Certificate('remitly-service-account.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

current_ref = db.collection('remitly_rates').document('current')
historical_ref = db.collection('remitly_rates').document('historical')
thresholds_ref = db.collection('remitly_rates').document('thresholds')


def scrape():
    response = requests.get(url)
    now = datetime.datetime.utcnow()
    if response.status_code == requests.codes.ok:
        soup = BeautifulSoup(response.text, "html.parser")
        a = soup.find_all('div', 'f1wm94yy fnsgms5')
        economy_price = float(a[6].contents[0][1:])
        historical_ref.set({
            now.strftime("%Y-%m-%d %H:%M"): economy_price
        }, merge=True)
        current_ref.set({
            'date': now.strftime("%Y-%m-%d"),
            'time': now.strftime("%H:%M"),
            'value': economy_price
        })
        doc = thresholds_ref.get()
        threshold_value = doc.to_dict()['cad_inr']
        if economy_price > threshold_value:
            r = client.chat_postMessage(
                channel='<channelID here>',
                text="CAD INR rate on Remitly is now:\n`1 CAD = "+str(economy_price)+" INR`")
            assert r['ok'], 'Error in sending message to slack'
    else:
        print(now.strftime("%Y-%m-%d %H:%M"), ' - Connection failed with remitly - ', response.status_code)


# Removed following listeners due to a bug. They stop working after 1 hr.
# Create a callback on_snapshot function to capture changes
# def on_snapshot(doc_snapshot, changes, read_time):
#     for doc in doc_snapshot:
#         print('{} price updated'.format(doc.id))
#         print(doc.to_dict())
#
#
# def on_snapshot2(doc_snapshot, changes, read_time):
#     for doc in doc_snapshot:
#         print('{} updated'.format(doc.id))
#         print(doc.to_dict())


# Watch the documents
# current_watch = current_ref.on_snapshot(on_snapshot)
# thresholds_watch = thresholds_ref.on_snapshot(on_snapshot2)

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(scrape, 'cron', minute='0')
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
