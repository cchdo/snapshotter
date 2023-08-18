import asyncio
from pathlib import Path
from collections import defaultdict, Counter
from zipfile import ZipFile, ZIP_DEFLATED
import csv

import aiohttp
from rich.progress import Progress

CCHDO_API_BASE = "https://cchdo.ucsd.edu"

# Limit how many parallel requests are going
aiohttp.TCPConnector(limit=20)

file_exts = {
    ("bottle", "cf_netcdf"): "_bottle.nc",
    ("bottle", "exchange"): "_hy1.csv",
    ("bottle", "whp_netcdf"): "_nc_hyd.zip",
    ("bottle", "woce"): "hy.txt",
    ("ctd", "cf_netcdf"): "_ctd.nc",
    ("ctd", "exchange"): "_ct1.zip",
    ("ctd", "whp_netcdf"): "_nc_ctd.zip",
    ("ctd", "woce"): "ct.zip",
    # ("trace_metals", "exchange"): "",
    # ("trace_metals", "whp_netcdf"): "",
    # ("trace_metals", "woce"): "",
    ("large_volume", "woce"): "lv.txt",
    ("documentation", "pdf"): "do.pdf",
    ("documentation", "text"): "do.txt",
    # ("documentation", "ods"): "do.ods",
    ("summary", "woce"): "su.txt",
}

get_files = defaultdict(dict)


def in_dataset(file):
    # we also ensure the files are public, just in case
    return (
        file["role"] == "dataset"
        and file["permissions"] == []
        and file["data_type"] != "trace_metals"
    )


snapshot = Path("snapshot")


def make_cruise_info(cruise) -> tuple[str, dict]:
    chisci = list(
        filter(lambda x: x["role"] == "Chief Scientist", cruise["participants"])
    )
    cochisci = list(
        filter(lambda x: x["role"] == "Co-Chief Scientist", cruise["participants"])
    )

    woce_lines = ", ".join(cruise["collections"]["woce_lines"])
    oceans = ", ".join(cruise["collections"]["oceans"])
    programs = ", ".join(cruise["collections"]["programs"])
    groups = ", ".join(cruise["collections"]["groups"])
    history_items = list(sorted(cruise["notes"], key=lambda x: x["date"]))

    history_block = []
    for note in history_items:
        history_block.append(f"Date: {note['date']}")
        history_block.append(f"From: {note['name']}")
        history_block.append(
            f"Subject: {note['data_type']} - {note['summary']} - {note['action']}"
        )
        history_block.append("")
        for line in note["body"]:
            history_block.append(line)

        history_block.append("")

    history = "\n".join(history_block)

    cruise_text = f"""{cruise['expocode']}
=============
Dates: {cruise['startDate']}/{cruise['endDate']}
Ship: {cruise['ship']}
Chief Scientist(s): {"; ".join([cs["name"] for cs in chisci])}
Co Chief Scientist(s): {"; ".join([cs["name"] for cs in cochisci])}
Country: {cruise["country"]}
WOCE Lines: {woce_lines}
Oceans: {oceans}
Programs: {programs}
Groups: {groups} 

History
-------
{history}
"""
    cruise_info = {
        **cruise,
        "woce_lines": woce_lines,
        "oceans": oceans,
        "programs": programs,
        "groups": groups,
    }
    return cruise_text, cruise_info


# Include woce sum files in other woce downloads


async def get_and_write_to_zip(
    session, fname, path, zf, progress, total, tasks, data_type, data_format
):
    async with session.get(path) as resp:
        zf.writestr(fname, await resp.read())
        progress.update(total, advance=1)
        progress.update(tasks[(data_type, data_format)], advance=1)


async def main():
    async with aiohttp.ClientSession(CCHDO_API_BASE) as session:
        async with session.get("/api/v1/cruise/all") as resp:
            crusies = await resp.json()
        async with session.get("/api/v1/file/all") as resp:
            files = await resp.json()

    file_by_id = {file["id"]: file for file in filter(in_dataset, files)}

    cruise_infos = []
    basins = Counter()
    programs = Counter()

    snapshot.mkdir(exist_ok=True)

    with ZipFile(
        snapshot / "data_info.zip", "w", compression=ZIP_DEFLATED, compresslevel=9
    ) as zf:
        for cruise in crusies:
            expocode = cruise["expocode"]
            cruise_text, cruise_info = make_cruise_info(cruise)
            cruise_infos.append(cruise_info)
            zf.writestr(f"{expocode}_info.txt", cruise_text)
            basins.update(cruise_info["collections"]["oceans"])
            programs.update(cruise_info["collections"]["programs"])
            for file_id in cruise["files"]:
                try:
                    file = file_by_id[file_id]
                except KeyError:
                    continue

                file_key = (file["data_type"], file["data_format"])
                if file_key not in file_exts:
                    continue

                fname = f"{expocode.replace('/', '_')}{file_exts[file_key]}"

                count = 2
                while fname in get_files[file_key]:
                    fname = f"{expocode.replace('/', '_')}_{count}{file_exts[file_key]}"
                    count += 1

                get_files[file_key][
                    fname
                ] = f"https://cchdo.ucsd.edu{file['file_path']}"

    with open(snapshot / "programs.csv", "w", newline="") as cs:
        columns = ("count", "program")
        writer = csv.DictWriter(cs, columns, extrasaction="ignore")
        writer.writeheader()
        for count, program in programs.most_common():
            writer.writerow({"count": count, "program": program})

    with open(snapshot / "basins.csv", "w", newline="") as cs:
        columns = ("count", "basins")
        writer = csv.DictWriter(cs, columns, extrasaction="ignore")
        writer.writeheader()
        for count, ocean in basins.most_common():
            writer.writerow({"count": count, "basins": ocean})

    with open(snapshot / "cruise_index.csv", "w", newline="") as cs:
        columns = (
            "expocode",
            "startDate",
            "endDate",
            "ship",
            "country",
            "woce_lines",
            "programs",
            "oceans",
            "groups",
        )

        writer = csv.DictWriter(cs, columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(sorted(cruise_infos, key=lambda x: x["expocode"]))

    with Progress() as progress:
        tasks = {}
        total = progress.add_task(
            "[red]Dowloading Files...", total=sum(len(f) for f in get_files.values())
        )
        for (data_type, data_format), files in get_files.items():
            tasks[(data_type, data_format)] = progress.add_task(
                f"[blue]{data_type} {data_format}", total=len(files)
            )

        for (data_type, data_format), files in get_files.items():
            path = snapshot / f"{data_type}_{data_format}.zip"
            path.parents[0].mkdir(parents=True, exist_ok=True)

            with ZipFile(path, "w", compression=ZIP_DEFLATED, compresslevel=9) as zf:
                async with aiohttp.ClientSession() as session:
                    aio_tasks = [
                        get_and_write_to_zip(
                            session,
                            fname,
                            path,
                            zf,
                            progress,
                            total,
                            tasks,
                            data_type,
                            data_format,
                        )
                        for fname, path in files.items()
                    ]
                    await asyncio.gather(*aio_tasks)


if __name__ == "__main__":
    asyncio.run(main())
