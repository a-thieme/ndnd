from pathlib import Path


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


def filter_replication(logs):
    out = []
    for line in logs:
        if "logjobs" in line:
            out.append(line)
    return out


class job_line:
    def __init__(self, line):
        self.line = line
        self.time = line.split("time=")[1].split(" ")[0]
        self.node = line.split("repo_")[1].split(".lg")[0]
        self.jobs = (
            line.split('jobs="')[1].split('" ')[0].replace("\t", " ").strip().split(" ")
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
        for job in get_jobs(running):
            times = 0
            for node in running:
                if job in running[node]:
                    times += 1
            appending.append(f"{line.time}\t{job}\t{times}")

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


import datetime


def convert_timestamp(stamp):
    return datetime.datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%S.%fZ")


def get_diffs(starts, replications):
    out = []
    for data_name in starts:
        for timestamp, replication in get_job_over_time(data_name, replications):
            diff = (
                convert_timestamp(timestamp) - convert_timestamp(starts[data_name])
            ).total_seconds()
            out.append(f"{diff}\t{replication}")
    return out


def rounding(diffs):
    from math import floor, log

    new = []
    for line in diffs:
        t, r = line.split("\t")
        t = float(t)
        if t == 0:
            continue
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


from statistics import mean, median


def get_averages(combined):
    out = []
    for ts in sorted(combined.keys()):
        out.append(f"{ts}\t{mean(combined[ts])}")
    return out


def get_medians(combined):
    out = []
    for ts in combined:
        out.append(f"{ts}\t{median(combined[ts])}")
    return out


def full_processing(ope):
    lines = read_all("running/logs")
    open("all_logs", ope).writelines(for_write(lines))
    job_logs = filter_replication(lines)
    jobs = [job_line(line) for line in job_logs]
    reps = get_times(jobs)
    open("jobs", ope).writelines(for_write(reps))
    start_times = get_starts(lines)
    diffs = get_diffs(start_times, reps)
    open("running/data/saved", ope).writelines(for_write(diffs))
    rounded = rounding(diffs)
    combined = combine(rounded)
    open("rounded", ope).writelines(for_write(diffs))
    open("running/data/combined", ope).writelines(for_write(diffs))
    open("running/data/average", ope).writelines(for_write(get_averages(combined)))
    open("running/data/median", ope).writelines(for_write(get_medians(combined)))


def big_wrapper():
    full_processing("a")
    c = open("running/data/combined").readlines()
    d = [line.strip() for line in c]
    e = rounding(d)
    f = combine(e)
    g = get_averages(f)
    open("big_combined", "w").writelines(for_write(g))


if __name__ == "__main__":
    print()
