from datetime import datetime, timedelta

# Starting point
start_time = datetime.strptime("2024-01-01 07:00:00", "%Y-%m-%d %H:%M:%S")

# Sample data
announcements_data = [
    61920, 534952, 298780, 926541, 947585,
    955020, 278340, 46908, 97468, 296142,
    274846, 615867, 98570, 889700, 97500,
    945514, 101051, 702476, 277788, 61989,
    96984, 811703, 182575, 278020
]

# Generate the x (time) and y (announcements) lists
x_times = [(start_time + timedelta(hours=i)).strftime('%Y-%m-%d %H:%M') for i in range(len(announcements_data))]
y_announcements = announcements_data

# x_times is your list of hourly time slots
# y_announcements is your list of announcement counts

print("X (Times):", x_times)
print("Y (Announcements):", y_announcements)
