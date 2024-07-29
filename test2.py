from datetime import datetime
from dateutil.tz import tzlocal

# Task start times
task_start_times = {
    'SplitFile-Lambda': [datetime(2024, 7, 24, 4, 13, 40, 906000, tzinfo=tzlocal())],
    'DataExtraction-Lambda': [
        datetime(2024, 7, 24, 4, 13, 43, 109000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 13, 43, 109000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 14, 54, 161000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 14, 54, 161000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 15, 56, 494000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 15, 57, 195000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 16, 59, 399000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 17, 0, 689000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 18, 1, 442000, tzinfo=tzlocal())
    ],
    'Transformation-Lambda': [
        datetime(2024, 7, 24, 4, 13, 45, 598000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 13, 45, 598000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 14, 54, 681000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 14, 54, 681000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 15, 56, 875000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 15, 57, 541000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 16, 59, 755000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 17, 1, 31000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 18, 1, 795000, tzinfo=tzlocal())
    ],
    'LoadDataLambda': [
        datetime(2024, 7, 24, 4, 13, 51, 658000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 13, 51, 658000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 14, 55, 232000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 14, 55, 368000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 15, 57, 369000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 15, 58, 216000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 17, 0, 457000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 17, 1, 641000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 18, 2, 247000, tzinfo=tzlocal())
    ],
    'BackupDataLambda': [
        datetime(2024, 7, 24, 4, 14, 53, 98000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 14, 53, 98000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 15, 56, 21000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 15, 56, 737000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 16, 58, 832000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 17, 0, 218000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 18, 1, 86000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 18, 2, 379000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 19, 2, 863000, tzinfo=tzlocal())
    ],
    'ProduceMetrics': [datetime(2024, 7, 24, 4, 19, 3, 318000, tzinfo=tzlocal())]
}

# Task end times
task_end_times = {
    'SplitFile-Lambda': [datetime(2024, 7, 24, 4, 13, 43, 109000, tzinfo=tzlocal())],
    'DataExtraction-Lambda': [
        datetime(2024, 7, 24, 4, 13, 45, 598000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 13, 45, 598000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 14, 54, 681000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 14, 54, 681000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 15, 56, 875000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 15, 57, 541000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 16, 59, 755000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 17, 1, 31000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 18, 1, 795000, tzinfo=tzlocal())
    ],
    'Transformation-Lambda': [
        datetime(2024, 7, 24, 4, 13, 51, 658000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 13, 51, 658000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 14, 55, 232000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 14, 55, 368000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 15, 57, 369000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 15, 58, 216000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 17, 0, 457000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 17, 1, 641000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 18, 2, 247000, tzinfo=tzlocal())
    ],
    'LoadDataLambda': [
        datetime(2024, 7, 24, 4, 14, 53, 98000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 14, 53, 98000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 15, 56, 21000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 15, 56, 737000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 16, 58, 832000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 17, 0, 218000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 18, 1, 86000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 18, 2, 379000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 19, 2, 863000, tzinfo=tzlocal())
    ],
    'BackupDataLambda': [
        datetime(2024, 7, 24, 4, 14, 54, 161000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 14, 54, 161000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 15, 56, 494000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 15, 57, 195000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 16, 59, 399000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 17, 0, 689000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 18, 1, 442000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 18, 2, 785000, tzinfo=tzlocal()),
        datetime(2024, 7, 24, 4, 19, 3, 318000, tzinfo=tzlocal())
    ],
    'ProduceMetrics': [datetime(2024, 7, 24, 4, 19, 4, 897000, tzinfo=tzlocal())]
}

# Calculate total execution time for each stage
total_execution_time = {}

for task, start_times in task_start_times.items():
    end_times = task_end_times.get(task, [])
    if len(start_times) == len(end_times):
        total_execution_time[task] = sum((end - start).total_seconds() for start, end in zip(start_times, end_times))
    else:
        total_execution_time[task] = None  # Handle case where the number of start and end times don't match

print(total_execution_time)
