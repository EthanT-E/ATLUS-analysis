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


GeV = 1.0

atom.available_releases()
atom.set_release('2025e-13tev-beta')

lumi = 36.6  # fb-1 # data size of the full release
fraction = 1.0  # reduce this is if you want the code to run quicker

# Select the skim to use for the analysis
skim = "exactly4lep"

defs = {
    r'Data': {'dids': ['data']},
    r'Background $Z,t\bar{t},t\bar{t}+V,VVV$': {'dids':  [410470, 410155, 410218,
                                                          410219, 412043, 364243,
                                                          364242, 364246, 364248,
                                                          700320, 700321, 700322,
                                                          700323, 700324, 700325], 'color': "#6b59d3"},  # purple
    r'Background $ZZ^{*}$':     {'dids': [700600], 'color': "#ff0000"},  # red
    r'Signal ($m_H$ = 125 GeV)':  {'dids': [345060, 346228, 346310, 346311, 346312,
                                            346340, 346341, 346342], 'color': "#00cdff"},  # light blue
}

samples = atom.build_dataset(defs, skim=skim, protocol='https', cache=True)


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


# Define empty list to hold all data for this sample
# We open an MC data file with simulated ttbar events (data set ID 410470)
value = samples[r'Background $Z,t\bar{t},t\bar{t}+V,VVV$']["list"][0]
print(value)

# This is now appended to our file path to retrieve the root file
background_ttbar_path = value

# Accessing the file from the online database
tree = uproot.open(background_ttbar_path + ":analysis")
weight_variables = ["filteff", "kfac", "xsec", "mcWeight", "ScaleFactor_PILEUP",
                    "ScaleFactor_ELE", "ScaleFactor_MUON", "ScaleFactor_LepTRIGGER"]

# For example, see below for the weights corresponding to muon rejection
tree["ScaleFactor_MUON"].arrays(library="ak")
print(weight_variables)
# luminosity of data_D
lumi = 0.105  # Luminosity of periodD in data 15 is 0.105 fb-1

# Let's use the first event of our tree
event = tree.arrays()[0]

# Multiply all the important weights together
total_weight = lumi * 1000 / event["sum_of_weights"]
for variable in weight_variables:
    total_weight = total_weight * event[variable]
print(f"{total_weight=}")


def calc_weight(weight_variables, events):
    total_weight = lumi * 1000 / events["sum_of_weights"]
    for variable in weight_variables:
        total_weight = total_weight * abs(events[variable])
    return total_weight


# Verify that we get the same answer
print(calc_weight(weight_variables, event))
sample_data = []

# Perform the cuts for each data entry in the tree
variables = ['lep_pt', 'lep_eta', 'lep_phi', 'lep_e', 'lep_charge', 'lep_type', 'trigE', 'trigM', 'lep_isTrigMatched',
             'lep_isLooseID', 'lep_isMediumID', 'lep_isLooseIso', 'lep_type']
for data in tree.iterate(variables + weight_variables+['sum_of_weights'], library="ak"):
    # Cuts
    lep_type = data['lep_type']
    data = data[~cut_lep_type(lep_type)]
    lep_charge = data['lep_charge']
    data = data[~cut_lep_charge(lep_charge)]

    # Invariant Mass
    data['mass'] = calc_mass(
        data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_e'])

    # Store Monte Carlo weights in the data
    data['totalWeight'] = calc_weight(weight_variables, data)
    # data['totalWeight'] = calc_weight(data)

    # Append data to the whole sample data list
    sample_data.append(data)

# turns sample_data back into an awkward array
background_ttbar = ak.concatenate(sample_data)
step_size = 2.5 * GeV

# define list to hold the Monte Carlo histogram entries
mc_x = ak.to_numpy(background_ttbar["mass"])
# define list to hold the Monte Carlo weights
mc_weights = ak.to_numpy(background_ttbar["totalWeight"])
# define list to hold the colors of the Monte Carlo bars
mc_colors = samples[r'Background $Z,t\bar{t},t\bar{t}+V,VVV$']['color']
# define list to hold the legend labels of the Monte Carlo bars
mc_labels = r'Background $Z,t\bar{t},t\bar{t}+V,VVV$'

lumi = 36.6

# Controls the fraction of all events analysed
# reduce this is if you want quicker runtime (implemented in the loop over the tree)
fraction = 1.0

# Define empty dictionary to hold awkward arrays
all_data = {}

# Workerrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr
connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

message = json.dumps(samples)

channel.queue_declare(queue='Atlus_samples')
channel.basic_publish(exchange='',
                      routing_key='Atlus_samples',
                      body=message,
                      properties=pika.BasicProperties(
                          delivery_mode=pika.DeliveryMode.Persistent)
                      )

connection = pika.BlockingConnection(pika.ConnectionParameters('RabbitMQ'))
channel = connection.channel()

channel.queue_declare(queue='return_data')


def callback(ch, method, properties, body):
    print("Recv")
    channel.stop_consuming()
    connection.close()


message = channel.basic_consume(queue='return_data',
                                auto_ack=True,
                                on_message_callback=callback)
channel.start_consuming()

samples = json.loads(message.decode())

xmin = 80 * GeV
xmax = 250 * GeV

bin_edges = np.arange(start=xmin,  # The interval includes this value
                      stop=xmax+step_size,  # The interval doesn't include this value
                      step=step_size)
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
# *************
bin_centres = np.arange(start=xmin+step_size/2,  # The interval includes this value
                        stop=xmax+step_size/2,  # The interval doesn't include this value
                        step=step_size)
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
         'for education',  # text
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
plt.savefig("/data/higgs.png")

# draw the legend
# no box around the legend
my_legend = main_axes.legend(frameon=False, fontsize=16)
# Signal stacked height
signal_tot = signal_heights[0] + mc_x_tot

# Peak of signal
print(signal_tot[18])

# Neighbouring bins
print(signal_tot[17:20])

# Signal and background events
N_sig = signal_tot[17:20].sum()
N_bg = mc_x_tot[17:20].sum()

# Signal significance calculation
signal_significance = N_sig/np.sqrt(N_bg + 0.3 * N_bg**2)  # EXPLAIN THE 0.3
print(f"\nResults:\n{N_sig=}\n{N_bg=}\n{signal_significance=}\n")
