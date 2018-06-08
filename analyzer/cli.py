import json
import os
import sys
from argparse import ArgumentParser
from functools import partial

from analyzer import LOGGER, classify, database, migrate_data, stats, util
from maya import MayaDT

SUB = 'subparser'
FILL = 'fill'
TEARDOWN = 'teardown'
GENERATE_CSV = 'generate-csv'
ANALYZE = 'analyze'
PLOT = 'plot'
ROWS_PER_FILE = 1000


def get_argparser():
    parser = ArgumentParser("analyzing stufz")
    parser.add_argument(
        '-d',
        '--driver',
        help="Choose a database driver",
        type=str,
        choices=[d.name for d in database.Driver],
        default=database.Driver.DEV.name)
    _add_subparsers(parser)
    return parser


def _add_subparsers(parser):
    subparsers = parser.add_subparsers(dest=SUB)
    subparsers.required = True
    subparsers.add_parser(TEARDOWN, help="Tear down the database.")
    _add_fill_subparser(subparsers)
    _add_generate_csv_subparser(subparsers)
    _add_analyze_subparser(subparsers)
    _add_plot_subparser(subparsers)


def _add_fill_subparser(subparsers):
    fill = subparsers.add_parser(FILL, help='Fill the database.')
    fill.add_argument(
        '-d',
        '--creation-date',
        help=
        "A date on the form 'yyyy-mm-dd'. Fill the database with documents starting from this date.",
        type=str,
        default=None)

    doc_type_grp = fill.add_argument_group()

    doc_type_grp.add_argument(
        '-q',
        '--questions-xml',
        help=("Path to an xml file with questions. Any row that is not a "
              "question will be ignored."),
        type=str)
    doc_type_grp.add_argument(
        '-a',
        '--answers-xml',
        help=("Path to an xml file with answers. Any row that is not an "
              "answer will be ignored."),
        type=str)
    doc_type_grp.add_argument(
        '-c',
        '--comments-xml',
        help=(
            "Path to the comments xml file. IMPORTANT: Comments will only be "
            "added for posts that exist in the database!"),
        type=str)


def _add_generate_csv_subparser(subparsers):
    gen = subparsers.add_parser(
        GENERATE_CSV, help="Generate CSV files that Senti4SD can process.")
    gen.add_argument(
        '-o', '--outpath', help="Path to output file", type=str, required=True)
    gen.add_argument(
        '-n',
        '--num',
        help="Amount of documents to pick",
        type=int,
        required=False)

    gen_grp = gen.add_mutually_exclusive_group(required=True)
    gen_grp.add_argument(
        '-a',
        '--answers',
        help="Generate an answers csv file",
        action='store_true')
    gen_grp.add_argument(
        '-c',
        '--comments',
        help="Generate a comments csv file",
        action='store_true')
    gen_grp.add_argument(
        '-q',
        '--questions',
        help="Generate a quiestions csv file",
        action='store_true')

    gen_sampling = gen.add_mutually_exclusive_group(required=False)
    gen_sampling.add_argument(
        '-t',
        '--tag',
        help=("A tag to filter by (e.g. 'python' or 'java'). There is only "
              "support for a single tag."),
        type=str)


def _add_analyze_subparser(subparsers):
    analyze = subparsers.add_parser(
        ANALYZE, help="Analyze a CSV file with Senti4SD")
    analyze.add_argument(
        '-rpf',
        '--rows-per-file',
        help=
        ("To avoid running out of memory, files are split into subfiles before "
         "being fed to Senti4SD. This option specifies the (max) amount of "
         "rows in each subfile."),
        type=int,
        default=ROWS_PER_FILE)
    analyze.add_argument(
        '-s',
        '--senti4sd-pool-root',
        help=
        "Filepath to a directory containing one or more copies of the Senti4SD github repo.",
        type=str,
        required=True)
    analyze.add_argument(
        '-i',
        '--input',
        help="Filepath to the CSV file to be analyzed",
        type=str,
        required=True)
    analyze.add_argument(
        '-o',
        '--output',
        help="Filepath to the output file",
        type=str,
        default='out.csv')


def _add_plot_subparser(subparsers):
    plot = subparsers.add_parser(
        PLOT,
        help="Plot sentiment probabilities given a list of prediction CSV files"
    )
    plot.add_argument(
        '-i',
        '--input',
        help="Prediction CSV files to be plotted",
        type=str,
        required=True,
        action='append')
    plot.add_argument(
        '-pop',
        '--population',
        help=(
            "An optional json file with subpopulations used "
            "to calculate confidence intervals. The key of the "
            "subpopulation must be prepended to the "
            "corresponding input file. For example, if the json "
            "looks like '{'javascript': 200, 'c': 100}', then the corresponding "
            "input files must be named javascript_something and "
            "c_something. If the population file is not given, "
            "all subpopulations default to Inf (a reasonable "
            "assumption if sample sizes are small compared to "
            "subpopulations)."),
        type=str,
        default=None)
    plot.add_argument(
        '-a',
        '--alpha-level',
        help="Alpha level (1-confidence level) of the analysis",
        type=float,
        default=0.05)
    plot.add_argument(
        '-o',
        '--output',
        help="Name to the output file",
        type=str,
        default='predictions_plot')
    plot.add_argument(
        '-w',
        '--width',
        help="Width of each sentiment class in the plot",
        type=float,
        default=0.5)
    plot.add_argument(
        '-f',
        '--fill',
        help="Whether the bars in the plot shall be filled with colors",
        action='store_true')
    plot.add_argument(
        '-p',
        '--patterns',
        help="Whether the bars in the plot shall have hatch patterns",
        action='store_true')


def parse_args(sys_args):
    parser = get_argparser()
    args = parser.parse_args(sys_args)
    if getattr(args,
               SUB) == FILL and not (args.questions_xml or args.answers_xml
                                     or args.comments_xml):
        raise SystemExit(
            "No XML file specified, fill has no data to work on.\nRun with -h for usage."
        )
    return args


def _init_database(args):
    driver = getattr(database.Driver, args.driver)
    database.setup_database(driver)
    database.setup_session(driver)
    return driver


def _handle_fill_parser(args):
    creation_date = None if not args.creation_date else MayaDT.from_iso8601(
        args.creation_date)
    migrate_data.fill_database(args.questions_xml, args.answers_xml,
                               args.comments_xml, creation_date)


def _handle_generate_csv_parser(args):
    if args.comments:
        generate_csv = util.generate_comments_csv
        generate_csv = partial(
            generate_csv, num_comments=args.num, outpath=args.outpath)
    elif args.questions or args.answers:
        generate_csv = util.generate_posts_csv
        generate_csv = partial(
            generate_csv, num_posts=args.num, outpath=args.outpath)
        generate_csv = partial(
            generate_csv,
            post_type=database.PostType.QUESTION
            if args.questions else database.PostType.ANSWER)

    generate_csv = partial(generate_csv, tag=args.tag)

    return generate_csv()


def _handle_analyze_parser(args):
    inpath = os.path.abspath(args.input)

    if args.rows_per_file <= 0:
        LOGGER.error(f"At least 1 row per file is required.")
        sys.exit(1)
    elif not os.path.isfile(inpath):
        LOGGER.error(f"File {inpath} does not exist!")
        sys.exit(1)

    outpath = os.path.abspath(args.output)
    senti4sd_pool_root = os.path.abspath(args.senti4sd_pool_root)
    classify.classify_sentiment(args.rows_per_file, senti4sd_pool_root, inpath,
                                outpath)


def _parse_population(population_file):
    with open(population_file, 'r') as f:
        population = json.load(f)
    return population


def _handle_plot_parser(args):
    for path in args.input:
        if not os.path.isfile(os.path.abspath(path)):
            LOGGER.error(f"File {path} does not exist.")
            sys.exit(1)
    if args.population:
        if not os.path.isfile(os.path.abspath(args.population)):
            LOGGER.error(f"File {path} does not exist.")
            sys.exit(1)
        args.population = _parse_population(args.population)

    stats.plot_predictions(args.input, args.alpha_level, args.output,
                           args.width, args.fill, args.patterns,
                           args.population)


def handle_parsed_args(args):
    driver = _init_database(args)

    if getattr(args, SUB) == FILL:
        _handle_fill_parser(args)
        LOGGER.info(f"Database {driver.name} filled")
    elif getattr(args, SUB) == TEARDOWN:
        database.teardown_database(driver)
        LOGGER.info(f"Database {driver.name} torn down")
    elif getattr(args, SUB) == GENERATE_CSV:
        LOGGER.info("Generating csv file ...")
        index_filepath = _handle_generate_csv_parser(args)
        LOGGER.info(
            f"File generated at {args.outpath} and index file at {index_filepath}!"
        )
    elif getattr(args, SUB) == ANALYZE:
        _handle_analyze_parser(args)
    elif getattr(args, SUB) == PLOT:
        _handle_plot_parser(args)
    else:  # impossible
        assert False
