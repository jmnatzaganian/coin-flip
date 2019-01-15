"""
Python 3.4+

Demonstrate how probability works by flipping a fair coin.

Explanation: https://techtorials.me/coin-flipping-probabilities/
"""

# Native imports
import os
import random
import multiprocessing as mp
import time
import pathlib
import csv


def flip_coin():
    """
    Perform a coin flip

    :returns: 'H' if heads, else 'T', uses a 50% probability
    """

    return 'H' if random.random() >= 0.5 else 'T'


def perform_trials(num_trials, q=None):
    """
    Flip a coin n times, and return the results

    :param num_trials: The number of times to flip the coin.
    :param q: The multiprocessing queue to use. If not supplied, it is ignored.
    :returns: Dictionary containing the trial results. Key is the chain length, value is a list containing the
    number of heads and tails, respectively.
    """

    head_chains = {}  # {chain_length: [num_heads, num_tails]}
    counter = 1
    for _ in range(num_trials):
        chain = [0, 0]
        if counter in head_chains:
            chain = head_chains[counter]
        else:
            head_chains[counter] = chain

        flip = flip_coin()
        if flip == 'H':
            chain[0] += 1
            counter += 1
        else:
            chain[1] += 1
            counter = 1

    if q is not None:
        q.put(head_chains)
    return head_chains


def merge_trials(trial_results):
    """
    Merges all provided trial results

    :param trial_results: A list of trial result objects
    :return: A new trial result object
    """

    d = {}
    for tr in trial_results:
        for k in tr:
            item = tr[k]
            if k in d:
                d[k][0] += item[0]
                d[k][1] += item[1]
            else:
                d[k] = item[:]
    return d


def yield_worker_batch_sizes(num_trials, num_workers):
    """
    Yields approximately even batch sizes. Items yielded first may have more items that later items.

    :param num_trials: The number of coin flips to perform
    :param num_workers: The number of workers to utilize
    :return: Yields a batch size
    """

    batch_size = num_trials // num_workers
    remainder = num_trials - batch_size * num_workers
    for i in range(num_workers):
        yield batch_size + int(i < remainder)


def main(out_file, num_trials=1000, num_workers=1):
    """
    Run the demo.

    :param out_file: Full path of where to save the CSV results.
    :param num_trials: Total number of trials to perform. One trial = one coin flip.
    :param num_workers: The number of workers to use. Each uses a separate process.
    :return:
    """

    # Safety
    if os.path.isfile(out_file):
        raise Exception(f'File {out_file} already exists - select a new destination')
    num_trials = int(num_trials)
    num_workers = int(num_workers)

    t = time.time()

    # Split up the work evenly across workers
    ctx = mp.get_context('spawn')
    q = ctx.Queue()
    procs = []
    for batch_size in yield_worker_batch_sizes(num_trials, num_workers):
        p = ctx.Process(target=perform_trials, args=(batch_size, q))
        p.start()
        procs.append(p)

    # Get the values
    results = []
    while len(results) < num_workers:
        results.append(q.get())

    # Close the processes
    for p in procs:
        p.join()

    print(f'It took {time.time() - t:.3f} seconds to perform {num_trials:,} coin flips')

    # Merge results
    merged_results = merge_trials(results)
    
    # Dump results to disk
    m = max(merged_results.keys())
    with open(out_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['chain_length', 'heads', 'tails'])
        for i in range(1, m + 1):
            num_heads, num_tails = merged_results.get(i, [0, 0])
            writer.writerow([i, num_heads, num_tails])

    print(f'Data dumped in {out_file}')


if __name__ == '__main__':
    nt = 1000000
    np = mp.cpu_count()
    op = os.path.join(str(pathlib.Path.home()), 'probability_data.csv')
    main(op, nt, np)
