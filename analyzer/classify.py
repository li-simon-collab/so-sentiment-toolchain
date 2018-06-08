"""Module containing functions for running the Senti4SD classificationTask.sh
script.

.. module:: classify
    :platform: Linux
    :synopsis: classificationTask related scripts.
.. moduleauthor:: Simon Larsén <slarse@kth.se>, Li Ling <liling@kth.se>
"""
import asyncio
import tempfile
import os
import shutil
from analyzer import LOGGER
from analyzer.util import (find_classifiers, concatenate_predictions,
                           split_file, log_exception)


class ClassificationError(Exception):
    """Raised when Senti4SD exits with a non-zero exit status."""
    pass


def classify_sentiment(rows_per_file: int, senti4sd_pool_root: str,
                       inpath: str, outpath: str):
    """Classify sentiment in the inpath file using any classificationTask.sh
    scripts found in the directory tree starting from senti4sd_pool_root.
    Result is placed at outpath.

    The input file will be split in subfiles of size rows_per_file to avoid
    running out of memory. This is a machinue and JVM-dependent figure and
    the optimal value will vary across setups.
    """
    classifiers = find_classifiers(senti4sd_pool_root)

    with tempfile.TemporaryDirectory() as tmpdir:
        sources = split_file(rows_per_file, inpath, dir=tmpdir)
        dests = [
            tempfile.NamedTemporaryFile(dir=tmpdir, delete=False).name
            for _ in range(len(sources))
        ]
        _classify_all(sources, dests, classifiers)
        concatenate_predictions(dests, outpath)


def _classify_all(sources, dests, classifiers):
    """Classify all files in sources and place outputs in destinations using
    the classifier scripts in classifiers."""
    src_dst_pairs = zip(sources, dests)
    num_classifiers = len(classifiers)

    loop = asyncio.get_event_loop()
    classified = 0
    num_files = len(sources)
    LOGGER.info(
        f"Classifying {num_files} subfiles with {num_classifiers} classifiers."
    )
    while classified < num_files:
        tasks = [
            loop.create_task(_async_classify_sentiment(classifier, src, dst))
            for classifier, (src, dst) in zip(classifiers, src_dst_pairs)
        ]
        loop.run_until_complete(asyncio.wait(tasks))
        for task in tasks:
            if task.exception():
                log_exception(f"Task {task} raised an exception",
                              task.exception())
                raise task.exception()
        classified += num_classifiers
        LOGGER.info(f"Subfiles classified: {classified}/{num_files}")
    loop.close()


async def _async_classify_sentiment(path_to_classifier: str, inpath: str,
                                    outpath: str):
    """Run the classification task asynchronously."""
    dir_name, script_name = os.path.split(path_to_classifier)
    out_file = os.path.basename(outpath)
    # the classification script must be run from the senti4SD directory
    command = ['/bin/bash', script_name, os.path.abspath(inpath), out_file]
    process = await asyncio.create_subprocess_exec(*command, cwd=dir_name)
    await process.communicate()
    if process.returncode != 0:
        with open(inpath, 'r') as f:
            LOGGER.error(
                f"Failed to classify {inpath} containing: {''.join(f.readlines())}"
            )
        raise ClassificationError(f"Classifying {inpath} failed.")
    shutil.move(os.path.join(dir_name, out_file), outpath)
