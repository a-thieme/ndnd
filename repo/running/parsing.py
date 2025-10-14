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

    return all_lines


def get_jobs(total):
    jobs = []
    for node in total:
        for job in total[node]:
            jobs.append(job)
    return list(set(jobs))


def filter_replication(logs):
    out = []
    for line in logs:
        if "logjobs" in line:
            out.append(line)
    return out


class job_line:
    def __init__(self, line):
        print(line)
        self.line = line
        self.time = line.split("time=")[1].split(" ")[0]
        self.node = line.split("repo_")[1].split(".lg")[0]
        self.jobs = (
            line.split('jobs="')[1].split('" ')[0].replace("\t", " ").strip().split(" ")
        )

    def __str__(self):
        return f"""line:\t{self.line}\n\ttime:\t{self.time}\n\tnode:\t{self.node}\n\tjobs:\t{self.jobs}"""


def get_times(lines: list[job_line]):
    times = {}


if __name__ == "__main__":
    lines = read_all("running/logs")
    job_logs = filter_replication(lines)
    jobs = [job_line(line) for line in job_logs]

# logs = []
# for line in open("parsed"):

# logs = sorted(logs)
# total = {}
# out = []
# for line in logs:
#     line = line.split("\t")
#     timestamp = line[0]
#     node = line[1]
#     jobs = line[2].strip().split(" ")
#     if jobs[-1] == '"':
#         jobs = jobs[0:-1]
#
#     total[node] = jobs
#     if len(jobs) == 0 or jobs[0] == "":
#         del total[node]
#     print(f"l:{line}")
#     print(f"t:{timestamp}")
#     print(f"n:{node}")
#     print(f"j:{jobs}")
#
#     for job in get_jobs(total):
#         times = 0
#         for node in total:
#             if job in total[node]:
#                 if job != "" and job != '"':
#                     times += 1
#         out.append(f"{timestamp}\t{job}\t{times}")
#
# for thing in out:
#     print(thing)
