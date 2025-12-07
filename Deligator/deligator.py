import atlasopenmagic as atom
import pika
import json
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import uproot
import awkward as ak
import vector
import requests
import uuid

MeV = 0.001
GeV = 1.0

atom.available_releases()
atom.set_release('2025e-13tev-beta')

# used Functions
# Cut lepton type (electron type is 11,  muon type is 13)


def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + \
        lep_type[:, 2] + lep_type[:, 3]
    lep_type_cut_bool = (sum_lep_type != 44) & (
        sum_lep_type != 48) & (sum_lep_type != 52)
    # True means we should remove this entry (lepton type does not match)
    return lep_type_cut_bool

# Cut lepton charge


def cut_lep_charge(lep_charge):
    # first lepton in each event is [:, 0], 2nd lepton is [:, 1] etc
    sum_lep_charge = lep_charge[:, 0] + lep_charge[:,
                                                   1] + lep_charge[:, 2] + lep_charge[:, 3] != 0
    # True means we should remove this entry (sum of lepton charges is not equal to 0)
    return sum_lep_charge

# Calculate invariant mass of the 4-lepton state
# [:, i] selects the i-th lepton in each event


def calc_mass(lep_pt, lep_eta, lep_phi, lep_e):
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_e})
    # .M calculates the invariant mass
    invariant_mass = (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M
    return invariant_mass


def cut_trig_match(lep_trigmatch):
    trigmatch = lep_trigmatch
    cut1 = ak.sum(trigmatch, axis=1) >= 1
    return cut1


def cut_trig(trigE, trigM):
    return trigE | trigM


def ID_iso_cut(IDel, IDmu, isoel, isomu, pid):
    thispid = pid
    return (ak.sum(((thispid == 13) & IDmu & isomu) | ((thispid == 11) & IDel & isoel), axis=1) == 4)


# Data_analysis
defs = {
    r'Data': {'dids': ['data']},
    r'Background $Z,t\bar{t},t\bar{t}+V,VVV$': {'dids': [410470, 410155, 410218,
                                                         410219, 412043, 364243,
                                                         364242, 364246, 364248,
                                                         700320, 700321, 700322,
                                                         700323, 700324, 700325], 'color': "#6b59d3"},  # purple
    r'Background $ZZ^{*}$':     {'dids': [700600], 'color': "#ff0000"},  # red
    r'Signal ($m_H$ = 125 GeV)':  {'dids': [345060, 346228, 346310, 346311, 346312,
                                            346340, 346341, 346342], 'color': "#00cdff"},  # light blue
}
skim = "exactly4lep"
samples = atom.build_dataset(defs, skim=skim, protocol='https', cache=True)
# Set luminosity to 36.6 fb-1, data size of the full release
lumi = 36.6

# Controls the fraction of all events analysed
# reduce this is if you want quicker runtime (implemented in the loop over the tree)
fraction = 1.0
weight_variables = ["filteff", "kfac", "xsec", "mcWeight", "ScaleFactor_PILEUP",
                    "ScaleFactor_ELE", "ScaleFactor_MUON", "ScaleFactor_LepTRIGGER"]
variables = ['lep_pt', 'lep_eta', 'lep_phi', 'lep_e', 'lep_charge', 'lep_type', 'trigE', 'trigM', 'lep_isTrigMatched',
             'lep_isLooseID', 'lep_isMediumID', 'lep_isLooseIso', 'lep_type']
# Define empty dictionary to hold awkward arrays
all_data = {}

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq', heartbeat=10000))
channel = connection.channel()

channel.queue_declare(queue='DataStream', durable=True)
channel.queue_declare(queue='reply', durable=True)
channel.exchange_declare('killer', exchange_type='fanout')


def callback(ch, method, properties, body):
    message = json.loads(body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Loop over samples
# Set luminosity to 36.6 fb-1, data size of the full release
lumi = 36.6

# Controls the fraction of all events analysed
# reduce this is if you want quicker runtime (implemented in the loop over the tree)
fraction = 1.0

# Define empty dictionary to hold awkward arrays
all_data = {}

# Loop over samples
for s in samples:

    # Print which sample is being processed
    print('Processing '+s+' samples')

    # Define empty list to hold data
    frames = []

    # Loop over each file
    for val in samples[s]['list']:
        # WorkerRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR
        # start the clock
        message = val
        id = str(uuid.uuid4())
        channel.basic_publish(exchange='', routing_key='DataStream', body=message,
                              properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent, correlation_id=id, reply_to='reply'))


def callback(ch, method, properties, body):
    inter = body.decode()
    message = json.loads(inter)
    frames.append(message)
    print(f"{len(frames)} all_data length")
    ch.basic_ack(delivery_tag=method.delivery_tag)


print("sent all")
channel.basic_consume(
    queue='reply', on_message_callback=callback)
channel.start_consuming()
print('all_rec')

# dictionary entry is concatenated awkward arrays
# all_data[s] = ak.concatenate(frames)
