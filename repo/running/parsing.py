logs = []
for line in open("parsed"):
    node = line.split("repo_")[1].split(".lg")[0]
    timestamp = line.split("time=")[1].split(" ")[0]
    jobs = line.split('jobs="')[1].split('" ')[0].replace("\t", " ")
    logs.append(f"{timestamp}\t{node}\t{jobs}")


def get_jobs(total):
    jobs = []
    for node in total:
        for job in total[node]:
            jobs.append(job)
    return list(set(jobs))


logs = sorted(logs)
total = {}
out = []
for line in logs:
    line = line.split("\t")
    timestamp = line[0]
    node = line[1]
    jobs = line[2].strip().split(" ")
    total[node] = jobs
    if jobs[0] == "":
        del total[node]

    print(f"l:{line}")
    print(f"t:{timestamp}")
    print(f"n:{node}")
    print(f"j:{jobs}")

    for job in get_jobs(total):
        times = 0
        for node in total:
            if job in total[node]:
                if job != "":
                    times += 1
        out.append(f"{timestamp}\t{job}\t{times}")

for thing in out:
    print(thing)
