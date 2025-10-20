from pathlib import Path
import json
from math import floor, log
from statistics import mean, median
import datetime
import numpy as np


def read_all(directory_path_str: str) -> list[str]:
    """
    Reads all files in a given directory and returns their content
    as a single list of lines.

    Args:
        directory_path_str: A string representing the path to the directory.

    Returns:
        A list where each element is a line from one of the files.
    """
    all_lines = []
    directory_path = Path(directory_path_str)

    # Check if the directory exists
    if not directory_path.is_dir():
        print(f"Error: Directory not found at '{directory_path_str}'")
        return []

    # Iterate through each file in the directory, sorted for consistency
    for file_path in sorted(directory_path.iterdir()):
        # Ensure we're only processing files, not subdirectories
        if file_path.is_file():
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    # extend() adds all items from the list to our main list
                    all_lines.extend([f"{file_path.name};{line.strip()}" for line in f])
            except Exception as e:
                print(f"Could not read file {file_path.name}: {e}")

    def get_timestamp(line):
        return line.split(";")[1].split(" ")[0]

    all_lines.sort(key=get_timestamp)
    return all_lines


def get_timestamp(line):
    return line.split(";")[1].split(" ")[0]


def filter_replication(logs):
    out = []
    still_looking = True
    for line in logs:
        if still_looking and "Error on face" in line:
            line += ' msg=logjobs jobs=""'
            still_looking = False
            print(line)
        if "logjobs" in line:
            out.append(line)
    return out


class job_line:
    def __init__(self, line):
        self.line = line
        self.time = line.split("time=")[1].split(" ")[0]
        self.node = line.split("repo_")[1].split(".lg")[0]
        if 'jobs=""' in line:
            self.jobs = []
        else:
            self.jobs = (
                line.split('jobs="')[1]
                .split('" ')[0]
                .replace("\t", " ")
                .strip()
                .split(" ")
            )
            if len(self.jobs[-1]) == 1:
                self.jobs = self.jobs[0:-1]

    def __str__(self):
        return f"""{self.time}\t{self.node}\t{self.jobs}"""


def get_jobs(total):
    jobs = []
    for node in total:
        for job in total[node]:
            jobs.append(job)
    return list(set(jobs))


def get_times(lines: list[job_line]):
    running = {}
    out = []
    for line in lines:
        running[line.node] = line.jobs
        if len(line.jobs) == 0:
            del running[line.node]

        appending = []
        # de-duplicate jobs from all logged into running
        for job in get_jobs(running):
            # check how many times job is currently done
            times = 0
            for node in running:
                if job in running[node]:
                    times += 1
            appending.append(f"{line.time}\t{job}\t{times}")

        # for each batch of data, sort by node to make it easier to read
        def get_node(line):
            return int(line.split("/")[3])

        appending.sort(key=get_node)
        out.extend(appending)
    return out


def for_write(lines):
    return [f"{line}\n" for line in lines]


def get_starts(lines):
    out = {}
    for line in lines:
        if "newTarget" in line:
            timestamp = line.split(";time=")[1].split()[0]
            data_name = line.split('newTarget="')[1].strip('"')
            out[data_name] = timestamp
    return out


def get_job_over_time(job, reps):
    out = []
    for rep in reps:
        if job in rep:
            timestamp, _, replications = rep.split("\t")
            out.append([timestamp, replications])
    return out


def convert_timestamp(stamp):
    return datetime.datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%S.%fZ")


def get_diffs(starts, replications, failure_time):
    if failure_time is None:
        failure_time = datetime.datetime(year=datetime.MAXYEAR, month=1, day=1)
    out = []
    after_failure = []
    total = []
    for data_name in starts:
        stime = convert_timestamp(starts[data_name])
        for timestamp, replication in get_job_over_time(data_name, replications):
            ts = convert_timestamp(timestamp)
            diff = (ts - stime).total_seconds()
            fail_diff = (ts - failure_time).total_seconds()
            if fail_diff >= 0:
                # fail_diff -= 20
                after_failure.append(f"{fail_diff}\t{replication}")
            else:
                out.append(f"{diff}\t{replication}")
            total.append(f"{diff}\t{replication}")

    def get_key(line):
        return float(line.split("\t")[0])

    return (
        sorted(out, key=get_key),
        sorted(after_failure, key=get_key),
        sorted(total, key=get_key),
    )


def rounding(diffs, zero_offset=0.01):
    new = []
    for line in diffs:
        t, r = line.split("\t")
        t = float(t)
        if t == 0:
            t = zero_offset
        precision = -1 * floor(log(t, 10))
        rounded = round(t, precision)
        new.append([rounded, r])
    return new


def combine(logs):
    out = {}
    for ts, rep in logs:
        if ts not in out:
            out[ts] = []
        out[ts].append(int(rep))
    return out


def get_averages(combined):
    out = []
    for ts in sorted(combined.keys()):
        out.append(f"{ts}\t{mean(combined[ts])}")
    return out


def get_medians(combined):
    out = []
    for ts in sorted(combined.keys()):
        out.append(f"{ts}\t{median(combined[ts])}")
    return out


def get_95percent(combined):
    out = []
    for ts in sorted(combined.keys()):
        out.append(f"{ts}\t{np.percentile(combined[ts], 5)}")
    return out


def get_downed_ts(log_dir):
    for line in read_all(log_dir):
        if "Error on face" in line:
            ts = get_timestamp(line).split("time=")[1]
            return ts


def full_processing():
    lines = read_all("running/logs")
    open("running/data/all_logs", "w").writelines(for_write(lines))

    job_logs = filter_replication(lines)
    jobs = [job_line(line) for line in job_logs]
    reps = get_times(jobs)
    open("running/data/jobs", "w").writelines(for_write(reps))

    start_times = get_starts(lines)
    dts = get_downed_ts("running/logs")
    if dts is None:
        fail_time = None
    else:
        fail_time = convert_timestamp(get_downed_ts("running/logs"))
    diffs, fail_diffs, all_diffs = get_diffs(start_times, reps, fail_time)

    # raw data
    open("running/data/all_diffs", "w").writelines(for_write(all_diffs))
    open("running/data/fail_diffs", "w").writelines(for_write(fail_diffs))
    open("running/data/before_diffs", "w").writelines(for_write(diffs))

    # put data into buckets by rounding timestamps, then have a dict[ts] = [list of reps for this ts]
    rounded = rounding(all_diffs)
    combined = combine(rounded)

    open("running/data/all_average", "w").writelines(for_write(get_averages(combined)))
    open("running/data/all_median", "w").writelines(for_write(get_medians(combined)))
    open("running/data/all_95_percent", "w").writelines(
        for_write(get_95percent(combined))
    )

    rounded = rounding(diffs)
    combined = combine(rounded)

    open("running/data/before_average", "w").writelines(
        for_write(get_averages(combined))
    )
    open("running/data/before_median", "w").writelines(for_write(get_medians(combined)))
    open("running/data/before_95_percent", "w").writelines(
        for_write(get_95percent(combined))
    )

    # put data into buckets by rounding timestamps, then have a dict[ts] = [list of reps for this ts]
    rounded = rounding(fail_diffs, 10)
    combined = combine(rounded)

    open("running/data/fail_average", "w").writelines(for_write(get_averages(combined)))
    open("running/data/fail_median", "w").writelines(for_write(get_medians(combined)))
    open("running/data/fail_95_percent", "w").writelines(
        for_write(get_95percent(combined))
    )
    calculate_max_seq("running/data")


def get_downed(lines):
    for line in lines:
        if "Error on face" in line:
            ts = get_timestamp(line).split("time=")[1]
            return ts


def calculate_max_seq(log_dir):
    filename = f"{log_dir}/all_logs"
    all = {}
    before = {}
    after = {}
    lines = open(filename).readlines()
    lines.sort(key=get_timestamp)
    ts = get_downed(lines)
    if ts is None:
        ts = datetime.datetime(year=datetime.MAXYEAR, month=1, day=1)
    else:
        ts = convert_timestamp(ts)
    print(ts)
    for line in lines:
        if "seq=" in line and "awareness update" in line:
            line = line.strip()
            node = line.split(" node=")[1]
            seq = line.split(" seq=")[1].split(" ")[0]
            t = convert_timestamp(get_timestamp(line).split("time=")[1])
            if (t - ts).total_seconds() < 0:
                before[node] = seq
            else:
                after[node] = seq
            all[node] = seq
    open(f"{log_dir}/before_seqs", "w").writelines(for_write(before.values()))
    open(f"{log_dir}/fail_seqs", "w").writelines(for_write(after.values()))
    open(f"{log_dir}/all_seqs", "w").writelines(for_write(all.values()))


def process_the_seqs():
    out = []
    # experiment, min, q1, median, q3, max
    for experiment in "1 4 8 12 16".split(" "):
        seqs = []
        for run in "one two three".split(" "):
            lines = open(f"running/{experiment}_timer/{run}/before_seqs").readlines()
            for x in lines:
                seqs.append(x)
        out.append(seqs)
        open(f"running/{experiment}_timer/seqs", "w").writelines(seqs)
    t = zip(*out)
    f = open("running/seqs", "w")
    for row in t:
        for val in row:
            f.write(val)
        f.write("\n")
    f.close()


if __name__ == "__main__":
    print()
