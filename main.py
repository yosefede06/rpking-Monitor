import pybgpstream
from datetime import datetime, timedelta
import csv
import plotly.graph_objects as go
from collections import defaultdict
from itertools import groupby
from operator import itemgetter
from tqdm import tqdm
import os


#
# ['rrc00', 'rrc01', 'rrc02', 'rrc03', 'rrc04', 'rrc05', 'rrc06',
#                              'rrc07', 'rrc08', 'rrc09', 'rrc10', 'rrc11', 'rrc13', 'rrc14', 'rrc15' 'rrc16', 'rrc17',
#                              'rrc18' 'rrc19', 'rrc20', 'rrc21','rrc22', 'rrc23', 'rrc24', 'rrc25','rrc26', 'rrc27']
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
                 collector= ['rrc00']):
        self.test_counter = 0
        self.test_dict = defaultdict(set)
        self.ip_version = ip_version
        self.start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        self.end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        self.total_time = self.end_time - self.start_time
        self.total_batches = self.total_time.total_seconds() / (batch_minutes * 60)
        self.stream = pybgpstream.BGPStream(
            from_time=start_time,
            until_time=end_time
        )
        filter_string = (f"elemtype announcements and collector {' '.join(collector)}"
                         f" and ipversion {ip_version} and type updates")
        self.stream.parse_filter_string(filter_string)
        self.metadata = defaultdict(lambda: defaultdict(set))
        self.batch_duration = timedelta(minutes=batch_minutes)
        self.directory = f'{start_time}_{end_time}_{batch_minutes}'
        self.filename = f'{self.directory}'
        self.file_path = f'logs/{self.directory}/{self.filename}'
        self.max_counts = []
        self.avg_counts = []  # Store average counts as well
        self.batch_labels = []
        if not os.path.exists(f'logs/{self.directory}'):
            os.mkdir(f"logs/{self.directory}")

    def process_stream(self):
        batch_start_time = None
        progress_bar = tqdm(total=int(self.total_batches), desc="Processing Batches")
        with open(f'{self.file_path}.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(
                ['Time', 'Max Diversity AS', 'Max Unique Paths', 'Max AS Prefixes', 'Highest Avg Diversity AS',
                 'Avg Unique Paths', 'Avg AS Prefixes'])

            for rec in self.stream:
                rec_time = datetime.utcfromtimestamp(rec.time)
                if batch_start_time is None:
                    batch_start_time = rec_time

                if rec_time - batch_start_time >= self.batch_duration:
                    self.process_batch(batch_start_time, writer)
                    batch_start_time = rec_time
                    progress_bar.update(1)  # Update progress for each batch processed

                self.update_metadata(rec)

    def process_batch(self, batch_start_time, writer):
        for key in self.test_dict:
            print(f"ASES {key}, PREFIXES  {str(len(self.test_dict[key]))}")
        print("Count " + str(self.test_counter))
        if self.metadata:
            max_processed_data = ProcessingStrategies.max_paths(self.metadata)
            avg_processed_data = ProcessingStrategies.average_paths(self.metadata)
            writer.writerow([
                batch_start_time.strftime('%Y-%m-%d %H:%M'),
                *max_processed_data,
                *avg_processed_data
            ])
            self.max_counts.append(max_processed_data[1])
            self.avg_counts.append(avg_processed_data[1])
            self.batch_labels.append(batch_start_time.strftime('%Y-%m-%d %H:%M'))

    def update_metadata(self, rec):
        elem = rec.get_next_elem()
        while elem:
            self.test_counter += 1
            hops = [k for k, g in groupby(elem.fields['as-path'].split(" "))]
            self.test_dict[hops[0]].add(elem.fields['prefix'])
            if len(hops) >= 1:
                self.metadata[int(hops[0])][elem.fields['prefix']].add(str(hops))
            elem = rec.get_next_elem()


class PlotCreator:
    def __init__(self, batch_labels, max_counts, avg_counts, file_path, ip_version=4):
        self.batch_labels = batch_labels
        self.max_counts = max_counts
        self.avg_counts = avg_counts  # New parameter for average counts
        self.ip_version = ip_version
        self.file_path = file_path

    def create_plot(self):
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=self.batch_labels, y=self.max_counts, mode='lines+markers', name='Maximal Path (Unique Paths per Prefix)'))
        fig.add_trace(go.Scatter(x=self.batch_labels, y=self.avg_counts, mode='lines+markers', name='Peak Average Diversity (Avg. Paths per AS)',
                                 line=dict(dash='dot')))  # Add the average count trace
        fig.update_layout(title=f'Identifying Extremes in BGP Path Diversity IPV{self.ip_version}:'
                                f' Maximum Unique Path Count for Prefixes vs. Highest Average Path Diversity for ASes',
                          xaxis_title='Data Collection Interval',
                          yaxis_title=f'Number of Paths',
                          xaxis=dict(tickmode='auto', nticks=len(self.batch_labels), tickangle=-45),
                          yaxis=dict(tickmode='auto'),
                          margin=dict(l=20, r=20, t=40, b=20),
                          paper_bgcolor="LightSteelBlue")
        fig.add_annotation(
            text='Data sourced from Collector: RRC0',
            align='left',
            showarrow=False,
            xref='paper',
            yref='paper',
            x=0,  # Adjust these values as needed to position your annotation
            y=1,
        )
        fig.write_html(f'{self.file_path}.html')


if __name__ == "__main__":
    processor = BGPDataProcessor("2024-01-01 00:00:00", "2024-01-01 00:30:00", batch_minutes=1)
    processor.process_stream()
    plotter = PlotCreator(processor.batch_labels, processor.max_counts, processor.avg_counts, processor.file_path)
    plotter.create_plot()