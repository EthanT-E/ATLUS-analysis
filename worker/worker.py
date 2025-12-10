'''
worker recv:
-samples

work send:
-all_data

'''
import atlasopenmagic as atom
import awkward as ak
import uproot
import vector
import time
import pika
import json


atom.available_releases()
atom.set_release('2025e-13tev-beta')


def calc_weight(weight_variables, events):
    total_weight = 36.6 * 1000 / events["sum_of_weights"]
    for variable in weight_variables:
        total_weight = total_weight * abs(events[variable])
    return total_weight


def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + \
        lep_type[:, 2] + lep_type[:, 3]
    lep_type_cut_bool = (sum_lep_type != 44) & (
        sum_lep_type != 48) & (sum_lep_type != 52)
    # True means we should remove this entry (lepton type does not match)
    return lep_type_cut_bool


def cut_lep_charge(lep_charge):
    # first lepton in each event is [:, 0], 2nd lepton is [:, 1] etc
    sum_lep_charge = lep_charge[:, 0] + lep_charge[:,
                                                   1] + lep_charge[:, 2] + lep_charge[:, 3] != 0
    # True means we should remove this entry (sum of lepton charges is not equal to 0)
    return sum_lep_charge


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


weight_variables = ["filteff", "kfac", "xsec", "mcWeight", "ScaleFactor_PILEUP",
                    "ScaleFactor_ELE", "ScaleFactor_MUON", "ScaleFactor_LepTRIGGER"]

variables = ['lep_pt', 'lep_eta', 'lep_phi', 'lep_e', 'lep_charge', 'lep_type', 'trigE', 'trigM', 'lep_isTrigMatched',
             'lep_isLooseID', 'lep_isMediumID', 'lep_isLooseIso', 'lep_type']

fraction = 1.0


def calc_data(fileString):
    s = ["Data",
         'Background $Z,t\bar{t},t\bar{t}+V,VVV$',
         "Background $ZZ^{*}$",
         "Signal ($m_H$ = 125 GeV)"]
    start = time.time()
    print("\t"+fileString+":")

    # Open file
    tree = uproot.open(fileString + ":analysis")

    sample_data = []

    # Loop over data in the tree
    for data in tree.iterate(variables + weight_variables + ["sum_of_weights", "lep_n"],
                             library="ak",
                             entry_stop=tree.num_entries*fraction):  # , # process up to numevents*fraction
        #  step_size = 10000000):

        # Number of events in this batch
        nIn = len(data)

        data = data[cut_trig(data.trigE, data.trigM)]
        data = data[cut_trig_match(data.lep_isTrigMatched)]

        # Record transverse momenta (see bonus activity for explanation)
        data['leading_lep_pt'] = data['lep_pt'][:, 0]
        data['sub_leading_lep_pt'] = data['lep_pt'][:, 1]
        data['third_leading_lep_pt'] = data['lep_pt'][:, 2]
        data['last_lep_pt'] = data['lep_pt'][:, 3]

        # Cuts on transverse momentum
        data = data[data['leading_lep_pt'] > 20]
        data = data[data['sub_leading_lep_pt'] > 15]
        data = data[data['third_leading_lep_pt'] > 10]

        data = data[ID_iso_cut(data.lep_isLooseID,
                               data.lep_isMediumID,
                               data.lep_isLooseIso,
                               data.lep_isLooseIso,
                               data.lep_type)]

        # Number Cuts
        # data = data[data['lep_n'] == 4]

        # Lepton cuts

        lep_type = data['lep_type']
        data = data[~cut_lep_type(lep_type)]
        lep_charge = data['lep_charge']
        data = data[~cut_lep_charge(lep_charge)]

        # Invariant Mass
        data['mass'] = calc_mass(
            data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_e'])

        # Store Monte Carlo weights in the data
        if 'data' not in s:  # Only calculates weights if the data is MC
            data['totalWeight'] = calc_weight(weight_variables, data)
            # data['totalWeight'] = calc_weight(data)

        # Append data to the whole sample data list
        sample_data.append(data)

        if not 'data' in fileString:
            # sum of weights passing cuts in this batch
            nOut = sum(data['totalWeight'])
        else:
            nOut = len(data)

        elapsed = time.time() - start  # time taken to process
        print("\t\t nIn: "+str(nIn)+",\t nOut: \t"+str(nOut)+"\t in " +
              str(round(elapsed, 1))+"s")  # events before and after

        sample_data = ak.concatenate(sample_data)
    return sample_data


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq', heartbeat=10000))
channel = connection.channel()

channel.queue_declare(queue='DataStream', durable=True)

channel.exchange_declare(exchange='killer', exchange_type='fanout')
killer_queue = channel.queue_declare(queue='', durable=True, exclusive=True)
kill_queue_name = killer_queue.method.queue
channel.queue_bind(queue=kill_queue_name, exchange='killer')


def DataStream_callback(ch, method, properties, body):
    message = body.decode()
    message = json.loads(message)
    # message_arr = message.split('<<<')
    # print(f"recv {message_arr[1]}")
    reply_message = {}
    reply_message["Data"] = calc_data(message["URL"])
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(len(reply_message["Data"]))
    if len(reply_message["Data"]) > 1000:
        size = len(reply_message["Data"])//10
        for i in range(0, len(reply_message["Data"]), size):
            print("chunk")
            temp_arr = {}
            temp_arr["Data"] = reply_message["Data"][i:i+size]
            temp_arr["Type"] = message["Type"]
            temp_arr["Data"] = ak.to_json(temp_arr["Data"])
            reply_chunk = json.dumps(temp_arr)
            ch.basic_publish(exchange='', routing_key=properties.reply_to, body=reply_chunk,
                             properties=pika.BasicProperties(correlation_id=properties.correlation_id))
    else:
        reply_message["Type"] = message["Type"]
        reply_message["Data"] = ak.to_json(reply_message["Data"])
        reply_message = json.dumps(reply_message)
        ch.basic_publish(exchange='', routing_key=properties.reply_to, body=reply_message,
                         properties=pika.BasicProperties(correlation_id=properties.correlation_id))


def kill_callback(ch, method, properties, body):
    print("killed")
    ch.basic_ack(delivery_tag=method.delivery_tag)
    channel.stop_consuming()
    connection.close()


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='DataStream',
                      on_message_callback=DataStream_callback)

channel.basic_consume(queue=kill_queue_name, on_message_callback=kill_callback)
print("[*] Start consuming")
channel.start_consuming()
print("Finished")
