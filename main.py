import pybgpstream
from datetime import datetime, timedelta
import csv
import plotly.graph_objects as go
from collections import defaultdict
from itertools import groupby
from operator import itemgetter
from tqdm import tqdm
#
# ['rrc00', 'rrc01', 'rrc02', 'rrc03', 'rrc04', 'rrc05', 'rrc06',
#                              'rrc07', 'rrc08', 'rrc09', 'rrc10', 'rrc11', 'rrc13', 'rrc14', 'rrc15' 'rrc16', 'rrc17',
#                              'rrc18' 'rrc19', 'rrc20', 'rrc21','rrc22', 'rrc23', 'rrc24', 'rrc25','rrc26', 'rrc27']
class ProcessingStrategies:
    @staticmethod
    def average_paths(metadata):
        averages = [(outer_key, sum(len(value) for value in inner_dict.values()) / len(inner_dict))
                    for outer_key, inner_dict in metadata.items()]
        max_entry = max(averages, key=itemgetter(1))
        return [(*max_entry, len(metadata[max_entry[0]]))]

    @staticmethod
    def count_unique_prefixes(metadata):
        counts = [(outer_key, len(inner_dict))
                  for outer_key, inner_dict in metadata.items()]
        max_entry = max(counts, key=itemgetter(1))
        return [(*max_entry, sum(len(value) for value in metadata[max_entry[0]].values()))]


class BGPDataProcessor:
    def __init__(self, start_time,
                 end_time,
                 batch_minutes=60,
                 ip_version=4,
                 collector= ['rrc00'],
                 process_function=ProcessingStrategies.average_paths):
        self.ip_version = ip_version
        self.start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        self.end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        self.total_time = self.end_time - self.start_time
        self.total_batches = self.total_time.total_seconds() / (batch_minutes * 60)
        self.stream = pybgpstream.BGPStream(
            from_time=start_time,
            until_time=end_time
        )
        self.process_function = process_function
        filter_string = (f"elemtype announcements and collector {' '.join(collector)}"
                         f" and ipversion {ip_version} and type updates")
        self.stream.parse_filter_string(filter_string)
        self.metadata = defaultdict(lambda: defaultdict(set))
        self.batch_duration = timedelta(minutes=batch_minutes)
        self.csv_file_path = f'logs/{start_time}_{end_time}_{batch_minutes}.csv'
        self.max_counts = []
        self.batch_labels = []

    def process_stream(self):
        batch_start_time = None
        progress_bar = tqdm(total=int(self.total_batches), desc="Processing Batches")
        with open(self.csv_file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Time', 'AS', 'MaxPaths', 'Prefixes'])  # CSV Header
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
        if self.metadata:
            processed_data = self.process_function(self.metadata)
            for data in processed_data:
                writer.writerow([batch_start_time.strftime('%Y-%m-%d %H:%M'), *data])
                self.max_counts.append(data[1])
            self.batch_labels.append(batch_start_time.strftime('%Y-%m-%d %H:%M'))
            # self.metadata.clear()

    def update_metadata(self, rec):
        elem = rec.get_next_elem()
        while elem:
            hops = [k for k, g in groupby(elem.fields['as-path'].split(" "))]
            if len(hops) >= 1:
                self.metadata[int(hops[0])][elem.fields['prefix']].add(str(hops))
            elem = rec.get_next_elem()


class PlotCreator:
    def __init__(self, batch_labels, max_counts, ip_version=4):
        self.batch_labels = batch_labels
        self.max_counts = max_counts
        self.ip_version = ip_version

    def create_plot(self):
        # Creating the plot
        fig = go.Figure()
        # Add line plot
        fig.add_trace(go.Scatter(x=self.batch_labels, y=self.max_counts, mode='lines+markers', name='Max Count'))

        # Update layout
        fig.update_layout(title=f'IPV{self.ip_version} - Average different Paths for'
                                f' same (Interface, Prefix) per Time Batch',
                          xaxis_title='Time Batch',
                          yaxis_title='Average Different Paths for same (Interface, Prefix)',
                          xaxis=dict(tickmode='auto', nticks=len(self.batch_labels), tickangle=-45),
                          yaxis=dict(tickmode='auto'),
                          margin=dict(l=20, r=20, t=40, b=20),
                          paper_bgcolor="LightSteelBlue")
        fig.show()


if __name__ == "__main__":
    processor = BGPDataProcessor("2024-02-01 07:00:00",
                                 "2024-02-02 07:00:00",
                                 batch_minutes=60)
    processor.process_stream()
    plotter = PlotCreator(processor.batch_labels, processor.max_counts)
    plotter.create_plot()
