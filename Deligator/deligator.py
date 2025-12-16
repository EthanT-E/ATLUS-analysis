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


def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + \
        lep_type[:, 2] + lep_type[:, 3]
    lep_type_cut_bool = (sum_lep_type != 44) & (
        sum_lep_type != 48) & (sum_lep_type != 52)
    return lep_type_cut_bool


def cut_lep_charge(lep_charge):
    # first lepton in each event is [:, 0], 2nd lepton is [:, 1] etc
    sum_lep_charge = lep_charge[:, 0] + lep_charge[:,
                                                   1] + lep_charge[:, 2] + lep_charge[:, 3] != 0
    # True means we should remove this entry (sum of lepton charges is not equal to 0)
    return sum_lep_charge


def calc_mass(lep_pt, lep_eta, lep_phi, lep_e):
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_e})
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

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq', heartbeat=10000))
channel = connection.channel()

channel.queue_declare(queue='DataStream', durable=True)
channel.queue_declare(queue='reply', durable=True)
channel.exchange_declare('killer', exchange_type='fanout')

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

    # Loop over each file
    for val in samples[s]['list']:
        # start the clock
        message = {}
        message["Type"] = s
        message["URL"] = val
        id = str(uuid.uuid4())
        body = json.dumps(message)
        channel.basic_publish(exchange='', routing_key='DataStream', body=body,
                              properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent, correlation_id=id, reply_to='reply'))

frames_data = []
frames_vvv = []
frames_zz = []
frames_signal = []

counter = 0


def callback(ch, method, properties, body):
    inter = body.decode()
    message = json.loads(inter)
    message["Data"] = ak.from_json(message["Data"])
    match message["Type"]:
        case 'Data':
            frames_data.append(message["Data"])
        case r'Background $Z,t\bar{t},t\bar{t}+V,VVV$':
            frames_vvv.append(message["Data"])
        case r'Background $ZZ^{*}$':
            frames_zz.append(message["Data"])
        case 'Signal ($m_H$ = 125 GeV)':
            if (len(message['Data']) != 0):
                frames_signal.append(message["Data"])
            else:
                print('empty message')
    global counter
    counter += 1
    if (counter == 120):
        channel.basic_publish(exchange='killer', routing_key='', body='kill',
                              properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))
        channel.stop_consuming()

    ch.basic_ack(delivery_tag=method.delivery_tag)


print("sent all")
channel.basic_consume(
    queue='reply', on_message_callback=callback)
channel.start_consuming()
print('all_rec')

all_data['Data'] = ak.concatenate(frames_data)
all_data[r'Background $Z,t\bar{t},t\bar{t}+V,VVV$'] = ak.concatenate(
    frames_vvv)
all_data[r'Background $ZZ^{*}$'] = ak.concatenate(frames_zz)
all_data[r'Signal ($m_H$ = 125 GeV)'] = ak.concatenate(frames_signal)


step_size = 2.5 * GeV
xmin = 80 * GeV
xmax = 250 * GeV
bin_edges = np.arange(start=xmin,  # The interval includes this value
                      stop=xmax+step_size,  # The interval doesn't include this value
                      step=step_size)  # Spacing between values

data_x, _ = np.histogram(ak.to_numpy(all_data['Data']['mass']),
                         bins=bin_edges)  # histogram the data
data_x_errors = np.sqrt(data_x)  # statistical error on the data

# histogram the signal
signal_x = ak.to_numpy(all_data[r'Signal ($m_H$ = 125 GeV)']['mass'])
# get the weights of the signal events
signal_weights = ak.to_numpy(all_data[r'Signal ($m_H$ = 125 GeV)'].totalWeight)
# get the colour for the signal bar
signal_color = samples[r'Signal ($m_H$ = 125 GeV)']['color']

mc_x = []  # define list to hold the Monte Carlo histogram entries
mc_weights = []  # define list to hold the Monte Carlo weights
mc_colors = []  # define list to hold the colors of the Monte Carlo bars
mc_labels = []  # define list to hold the legend labels of the Monte Carlo bars

for s in samples:  # loop over samples
    if s not in ['Data', r'Signal ($m_H$ = 125 GeV)']:  # if not data nor signal
        # append to the list of Monte Carlo histogram entries
        mc_x.append(ak.to_numpy(all_data[s]['mass']))
        # append to the list of Monte Carlo weights
        mc_weights.append(ak.to_numpy(all_data[s].totalWeight))
        # append to the list of Monte Carlo bar colors
        mc_colors.append(samples[s]['color'])
        mc_labels.append(s)  # append to the list of Monte Carlo legend labels


# *************
# Main plot
#

xmin = 80 * GeV
xmax = 250 * GeV

# Histogram bin setup
step_size = 2.5 * GeV
bin_edges = np.arange(start=xmin,  # The interval includes this value
                      stop=xmax+step_size,  # The interval doesn't include this value
                      step=step_size)  # Spacing between values
bin_centres = np.arange(start=xmin+step_size/2,  # The interval includes this value
                        stop=xmax+step_size/2,  # The interval doesn't include this value
                        step=step_size)  # Spacing between values *************
fig, main_axes = plt.subplots(figsize=(12, 8))

# plot the data points
main_axes.errorbar(x=bin_centres, y=data_x, yerr=data_x_errors,
                   fmt='ko',  # 'k' means black and 'o' is for circles
                   label='Data')

# plot the Monte Carlo bars
mc_heights = main_axes.hist(mc_x, bins=bin_edges,
                            weights=mc_weights, stacked=True,
                            color=mc_colors, label=mc_labels)

mc_x_tot = mc_heights[0][-1]  # stacked background MC y-axis value

# calculate MC statistical uncertainty: sqrt(sum w^2)
mc_x_err = np.sqrt(np.histogram(
    np.hstack(mc_x), bins=bin_edges, weights=np.hstack(mc_weights)**2)[0])

# plot the signal bar
signal_heights = main_axes.hist(signal_x, bins=bin_edges, bottom=mc_x_tot,
                                weights=signal_weights, color=signal_color,
                                label=r'Signal ($m_H$ = 125 GeV)')

# plot the statistical uncertainty
main_axes.bar(bin_centres,  # x
              2*mc_x_err,  # heights
              alpha=0.5,  # half transparency
              bottom=mc_x_tot-mc_x_err, color='none',
              hatch="////", width=step_size, label='Stat. Unc.')

# set the x-limit of the main axes
main_axes.set_xlim(left=xmin, right=xmax)

# separation of x axis minor ticks
main_axes.xaxis.set_minor_locator(AutoMinorLocator())

# set the axis tick parameters for the main axes
main_axes.tick_params(which='both',  # ticks on both x and y axes
                      direction='in',  # Put ticks inside and outside the axes
                      top=True,  # draw ticks on the top axis
                      right=True)  # draw ticks on right axis

# x-axis label
main_axes.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]',
                     fontsize=13, x=1, horizontalalignment='right')

# write y-axis label for main axes
main_axes.set_ylabel('Events / '+str(step_size)+' GeV',
                     y=1, horizontalalignment='right')

# set y-axis limits for main axes
main_axes.set_ylim(bottom=0, top=np.amax(data_x)*2.0)

# add minor ticks on y-axis for main axes
main_axes.yaxis.set_minor_locator(AutoMinorLocator())

# Add text 'ATLAS Open Data' on plot
plt.text(0.1,  # x
         0.93,  # y
         'ATLAS Open Data',  # text
         transform=main_axes.transAxes,  # coordinate system used is that of main_axes
         fontsize=16)

# Add text 'for education' on plot
plt.text(0.1,  # x
         0.88,  # y
         'for education, used by Ethan Tripp-Edwards',  # text
         transform=main_axes.transAxes,  # coordinate system used is that of main_axes
         style='italic',
         fontsize=12)

# Add energy and luminosity
lumi_used = str(lumi*fraction)  # luminosity to write on the plot
plt.text(0.1,  # x
         0.82,  # y
         r'$\sqrt{s}$=13 TeV,$\int$L dt = '+lumi_used+' fb$^{-1}$',  # text
         # coordinate system used is that of main_axes
         transform=main_axes.transAxes, fontsize=16)

# Add a label for the analysis carried out
plt.text(0.1,  # x
         0.76,  # y
         r'$H \rightarrow ZZ^* \rightarrow 4\ell$',  # text
         # coordinate system used is that of main_axes
         transform=main_axes.transAxes, fontsize=16)

# draw the legend
# no box around the legend
my_legend = main_axes.legend(frameon=False, fontsize=16)
plt.savefig("/data/plot.png")  # save fig in volume


# Signal stacked height
signal_tot = signal_heights[0] + mc_x_tot

# Signal and background events
N_sig = signal_tot[17:20].sum()
N_bg = mc_x_tot[17:20].sum()

# Signal significance calculation save to file
signal_significance = N_sig/np.sqrt(N_bg + 0.3 * N_bg**2)
with open(r"/data/results_file.txt", "w+") as f:
    f.write(f"Peak of signal: {signal_tot[18]}")
    f.write(f"\nNeighbouring bins: {signal_tot[17:20]}")
    f.write(f"\nResults:\n{N_sig=}\n{N_bg=}\n{signal_significance=}\n")
