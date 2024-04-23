import pybgpstream
from datetime import datetime, timedelta
import csv
import plotly.graph_objects as go
from collections import defaultdict
from itertools import groupby
from operator import itemgetter
from tqdm import tqdm
import os


all_collectors=['rrc00', 'rrc01', 'rrc02', 'rrc03', 'rrc04', 'rrc05', 'rrc06',
                             'rrc07', 'rrc08', 'rrc09', 'rrc10', 'rrc11', 'rrc13', 'rrc14', 'rrc15' 'rrc16', 'rrc17',
                             'rrc18' 'rrc19', 'rrc20', 'rrc21','rrc22', 'rrc23', 'rrc24', 'rrc25','rrc26', 'rrc27']
class ProcessingStrategies:

    @staticmethod
    def average_paths(metadata):
        averages = [(outer_key, round(sum(len(value) for value in inner_dict.values()) / len(inner_dict), 2))
                    for outer_key, inner_dict in metadata.items()]
        max_entry = max(averages, key=itemgetter(1))
        return *max_entry, len(metadata[max_entry[0]])

    @staticmethod
    def max_paths(metadata):
        maximum = [(outer_key, max([len(value) for value in inner_dict.values()]))
                    for outer_key, inner_dict in metadata.items()]
        max_entry = max(maximum, key=itemgetter(1))
        return *max_entry, len(metadata[max_entry[0]])


class BGPDataProcessor:

    def __init__(self, start_time,
                 end_time,
                 batch_minutes=60,
                 ip_version=4,
                 collector=all_collectors):
        self.collectors = collector
        self.ip_version = ip_version
        self.start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        self.end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        self.total_time = self.end_time - self.start_time
        self.total_batches = self.total_time.total_seconds() / (batch_minutes * 60)
        self.stream = pybgpstream.BGPStream(
            from_time=start_time,
            until_time=end_time
        )
        # elemtype announcements and
        filter_string = (f"elemtype announcements and collector {' '.join(collector)}"
                         f" and ipversion {ip_version} and type updates")
        self.stream.parse_filter_string(filter_string)
        self.collectors_metadata = {}
        self.max_counts = {}
        self.avg_counts = {}  # Store average counts as well
        self.batch_labels = {}
        for c in collector:
            self.collectors_metadata[c] = defaultdict(lambda: defaultdict(set))
            self.max_counts[c] = []
            self.avg_counts[c] = []  # Store average counts as well
            self.batch_labels[c] = []
        self.batch_duration = timedelta(minutes=batch_minutes)
        self.directory = f'all_{start_time}_{end_time}_{batch_minutes}'
        self.filename = f'{self.directory}'
        self.file_path = f'logs/{self.directory}/{self.filename}'

        if not os.path.exists(f'logs/{self.directory}'):
            os.mkdir(f"logs/{self.directory}")

    def process_stream(self):
        batch_start_time = None
        progress_bar = tqdm(total=int(self.total_batches), desc="Processing Batches")
        with open(f'{self.file_path}.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(
                ["Collector", 'Time', 'Max Diversity AS', 'Max Unique Paths', 'Max AS Prefixes', 'Highest Avg Diversity AS',
                 'Avg Unique Paths', 'Avg AS Prefixes'])

            for rec in self.stream:
                rec_time = datetime.utcfromtimestamp(rec.time)
                if batch_start_time is None:
                    batch_start_time = rec_time
                if rec_time - batch_start_time >= self.batch_duration:
                    for c in self.collectors:
                        self.process_batch(batch_start_time, writer, c)
                    batch_start_time = rec_time
                    progress_bar.update(1)  # Update progress for each batch processed
                self.update_metadata(rec)

    def process_batch(self, batch_start_time, writer, collector):
        if self.collectors_metadata[collector]:
            max_processed_data = ProcessingStrategies.max_paths(self.collectors_metadata[collector])
            avg_processed_data = ProcessingStrategies.average_paths(self.collectors_metadata[collector])
            writer.writerow([collector, batch_start_time.strftime('%Y-%m-%d %H:%M'), *max_processed_data,
                             *avg_processed_data
                             ])
            self.max_counts[collector].append(max_processed_data[1])
            self.avg_counts[collector].append(avg_processed_data[1])
            self.batch_labels[collector].append(batch_start_time.strftime('%Y-%m-%d %H:%M'))

    def update_metadata(self, rec):
        elem = rec.get_next_elem()
        while elem:
            # print(elem.type, end="\t")
            # print(elem.peer_asn, end="\t")
            # print(elem.fields['as-path'])
            hops = [k for k, g in groupby(elem.fields['as-path'].split(" "))]
            # self.test_dict[hops[0]].add(elem.fields['prefix'])
            if len(hops) >= 1:
                self.collectors_metadata[rec.collector][int(hops[0])][elem.fields['prefix']].add(str(hops))
            elem = rec.get_next_elem()


class PlotCreator:
    def __init__(self, batch_labels, max_counts, avg_counts, file_path, collectors, ip_version=4):
        self.collectors = collectors
        self.batch_labels = batch_labels
        self.max_counts = max_counts
        self.avg_counts = avg_counts
        self.ip_version = ip_version
        self.file_path = file_path

    def create_plot(self):
        fig = go.Figure()
        color_cycle = fig.layout.template.layout.colorway  # Get default color cycle from the current template
        for index, c in enumerate(self.collectors):
            color_index = index % len(color_cycle)
            fig.add_trace(go.Scatter(x=self.batch_labels[c],
                                     y=self.max_counts[c],
                                     mode='lines+markers',
                                     name=f'{c} - Maximal Path (Unique Paths per Prefix)',
                          line=dict(color=color_cycle[color_index])))

            fig.add_trace(go.Scatter(x=self.batch_labels[c],
                                     y=self.avg_counts[c],
                                     mode='lines+markers',
                                     name=f'{c} - Peak Average Diversity (Avg. Paths per AS)',
                                     line=dict(color=color_cycle[color_index], dash='dot')))  # Add the average count trace

        # fig.update_layout(title=f'Identifying Extremes in BGP Path Diversity IPV{self.ip_version}:'
        #                         f' Maximum Unique Path Count for Prefixes vs. Highest Average Path Diversity for ASes',
        #                   xaxis_title='Data Collection Interval',
        #                   yaxis_title=f'Number of Paths',
        #                   xaxis=dict(tickmode='auto', nticks=len(self.batch_labels), tickangle=-45),
        #                   yaxis=dict(tickmode='auto'),
        #                   margin=dict(l=20, r=20, t=40, b=20),
        #                   paper_bgcolor="LightSteelBlue")
        fig.add_annotation(
            text=f'Data sourced from Collectors: {", ".join(self.collectors)}',
            align='left',
            showarrow=False,
            xref='paper',
            yref='paper',
            x=0,
            y=1,
        )
        fig.write_html(f'{self.file_path}.html')


if __name__ == "__main__":
    # processor = BGPDataProcessor("2024-02-01 00:00:00", "2024-02-02 00:00:00", batch_minutes=15)
    processor = BGPDataProcessor("2024-02-01 00:00:00", "2024-02-02 00:00:00", batch_minutes=5)
    processor.process_stream()
    plotter = PlotCreator(processor.batch_labels, processor.max_counts, processor.avg_counts, processor.file_path, processor.collectors)
    plotter.create_plot()