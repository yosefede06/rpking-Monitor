# import networkx as nx
from collections import defaultdict
from itertools import groupby
import pybgpstream
from datetime import datetime, timedelta
import plotly.graph_objects as go
from operator import itemgetter
import csv

import numpy as np  # Import numpy for numerical operations
stream = pybgpstream.BGPStream(
    # Consider this time interval:
    # Sat, 01 Aug 2015 7:50:00 GMT -  08:10:00 GMT
    from_time="2024-01-01 07:00:00", until_time="2024-01-02 07:00:00"
    # collectors=["rrc00"],
)
# aspath ^61218_24961
filter_string = "elemtype announcements and collector rrc00 and ipversion 4 and type updates"

# Consider BGP updates
# stream.add_filter('record-type', 'updates')

# Filter by IPv4 prefixes
# stream.add_filter('ipversion', '4')
stream.parse_filter_string(filter_string)

metadata_ases = defaultdict(set)
metadata = defaultdict(lambda: defaultdict(set))
batch_start_time = None
batch_duration = timedelta(hours=1)
max_counts = []  # Store the max count for each batch
batch_labels = []  # Store the labels for each batch (start time)
fig = go.Figure()
# 61218->24961
# max_value = set()
csv_file_path = 'bgp_data_batches.csv'
# Process the stream
with open(csv_file_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Time', 'Interface', 'Announcements'])  # CSV Header
    for rec in stream:
        rec_time = datetime.utcfromtimestamp(rec.time)
        if batch_start_time is None:
            batch_start_time = rec_time

        if rec_time - batch_start_time >= batch_duration:
            # Process data for the current batch
            if metadata:  # Ensure metadata is not empty
                max_entry = max(
                    (((outer_key, inner_key), len(value)) for outer_key, inner_dict in metadata.items() for inner_key, value in
                     inner_dict.items()), key=itemgetter(1))
                ((max_outer_key, max_inner_key), max_value) = max_entry
                print("[" + str(max_outer_key) + "," + str(max_inner_key) + "]" + ";ANNOUNCEMENTS: " + str(max_value))
                writer.writerow([batch_start_time.strftime('%Y-%m-%d %H:%M'), str(max_outer_key) + "-" +
                                 str(max_inner_key), str(max_value)])
                metadata = defaultdict(lambda: defaultdict(set))
            # print(len(max_value))
                max_counts.append(max_value)
                batch_labels.append(batch_start_time.strftime('%Y-%m-%d %H:%M'))
            batch_start_time = rec_time

        elem = rec.get_next_elem()
        while elem:
            hops = [k for k, g in groupby(elem.fields['as-path'].split(" "))]
            if len(hops) >= 2:
                # max_value.add(elem.fields['prefix'])
                metadata[int(hops[0])][int(hops[1])].add(elem.fields['prefix'])
            elem = rec.get_next_elem()

transformed_max_counts = [2**np.ceil(np.log2(count)) if count > 0 else 1 for count in max_counts]

# Creating the plot
fig = go.Figure()

# Add trace
fig.add_trace(go.Scatter(x=batch_labels, y=transformed_max_counts, mode='lines+markers', name='Max Count'))

# Update layout for a better visual representation, correcting the logarithmic scale setup
fig.update_layout(
    title='Max AS Pair Counts per Time Batch (2^n Scale)',
    xaxis_title='Time Batch',
    yaxis_title='Max Element Count (2^n)',
    yaxis=dict(
        type='log',
        autorange=True,
        tickvals=[2**i for i in range(int(np.log2(min(transformed_max_counts))), int(np.ceil(np.log2(max(transformed_max_counts)))) + 1)],
        ticktext=[f'2^{i}' for i in range(int(np.log2(min(transformed_max_counts))), int(np.ceil(np.log2(max(transformed_max_counts)))) + 1)]
    ),
    xaxis=dict(tickmode='auto', nticks=len(batch_labels), tickangle=-45),
    paper_bgcolor="LightSteelBlue", margin=dict(l=20, r=20, t=40, b=20),
)

# Rotate labels to avoid overlap
fig.update_xaxes(tickangle=45)
for key in metadata:
    print(f"AS: {key}, Number of Peers: {len(metadata[key])}")
fig.show()

# # Add line plot
# fig.add_trace(go.Scatter(x=batch_labels, y=max_counts, mode='lines+markers', name='Max Count'))
#
# # Update layout
# fig.update_layout(title='Max AS Pair Counts per Time Batch',
#                   xaxis_title='Time Batch',
#                   yaxis_title='Max Element Count',
#                   xaxis=dict(tickmode='auto', nticks=len(batch_labels), tickangle=-45),
#                   yaxis=dict(tickmode='auto'),
#                   margin=dict(l=20, r=20, t=40, b=20),
#                   paper_bgcolor="LightSteelBlue")
#
