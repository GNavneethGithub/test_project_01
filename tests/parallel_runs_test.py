from multiprocessing import Pool

def func(record, config, thread_number):
    import subprocess
    # example subprocess call
    result = subprocess.run(
        ["echo", f"Record={record}, Config={config.get('name')}, Thread={thread_number}"],
        capture_output=True, text=True
    )
    print(result.stdout.strip())
    return record

def main(records, config, num_workers):
    with Pool(processes=num_workers) as pool:
        args = [(record, config, i) for i, record in enumerate(records, start=1)]
        results = pool.starmap(func, args)
    return results

if __name__ == "__main__":
    records = [{"id": i} for i in range(10)]
    config = {"name": "test_config"}
    main(records, config, num_workers=4)
