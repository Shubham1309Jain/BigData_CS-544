from datasets import load_dataset  # Import Hugging Face's datasets library
from pathlib import Path  # For handling file paths in a cross-platform way
import tempfile  # For creating temporary directories
import random  # For random sampling and shuffling
import json  # For reading/writing CSV files
from collections import defaultdict  # For grouping solutions by language
from tqdm import tqdm

# Get the directory where the script is located
current_file = Path(__file__).parent

# Check if the script is being run from the correct directory
if not current_file.name.startswith("p5"):
    print("Please run this script from the p5 directory!!")
    print(f"Current directory: {current_file.absolute()}")
    exit(1)

# Check if the required 'nb' directory exists
if not Path("nb").exists():
    print("No 'nb' directory found. Refer to the README.md and make it.")
    exit(1)

# Check if the required 'nb/data' directory exists
if not Path("nb/data").exists():
    print("No 'nb/data' directory found. Refer to the README.md and make it.")
    exit(1)

print("Splitting the CodeContests dataset into 'problems.jsonl' and 'solutions.jsonl'")
SEED = 42  # Set a random seed for reproducibility
random.seed(SEED)

# Define a mapping from numerical language IDs to human-readable language names
LANGUAGE_MAP = {
    0: "UNKNOWN_LANGUAGE",
    1: "PYTHON2",
    2: "CPP",
    3: "PYTHON3",
    4: "JAVA",
}
with open("nb/data/languages.csv", "w") as f:
    f.write("language,language_name\n")
    for k, v in LANGUAGE_MAP.items():
        f.write(f"{k},{v}\n")

SOURCE_MAP = {
    0: "UNKNOWN_SOURCE",
    1: "CODECHEF",
    2: "CODEFORCES",
    3: "HACKEREARTH",
    4: "CODEJAM",
    5: "ATCODER",
    6: "AIZU",
}


with open("nb/data/sources.csv", "w") as f:
    f.write("source,source_name\n")
    for k, v in SOURCE_MAP.items():
        f.write(f"{k},{v}\n")

# Define the set of keys that will be extracted for problem data
problem_keys = {
    "name",
    "source",
    "difficulty",
    "cf_contest_id",
    "cf_index",
    "cf_points",
    "cf_rating",
    "is_description_translated",
    "memory_limit_bytes",
}

TAG_MAPS = {}
PROB_ID_MAP = {}
# Define output file paths
problems_path = Path("nb/data/problems.jsonl")
solutions_path = Path("nb/data/solutions.jsonl")
num_removed = 0
prob_id_counter = 0
# Create a temporary directory to download and cache the dataset
with tempfile.TemporaryDirectory() as tmpdirname:
    # Load the DeepMind Code Contests dataset

    dataset = load_dataset(
        "deepmind/code_contests",
        split="train",  # Use the training split
        streaming=True,  # Stream the dataset to handle its large size
        cache_dir=tmpdirname,  # Store the cache in the temporary directory
    )
    dataset = dataset.shuffle(SEED)
    with Path("nb/data/problem_tests.csv").open("w") as test_file:
        test_file.write(
            "problem_id,input_chars,output_chars,is_public,is_generated,is_private,output_is_number\n"
        )
        # Open both output files for writing
        with problems_path.open("w") as problems_fd:
            with solutions_path.open("w") as solutions_fd:
                problems_saved = 0  # Counter for saved problems
                # Process each problem in the dataset
                for task in tqdm(dataset, total=10_000, desc="Processing problems"):
                    # Extract problem data for the relevant keys

                    problem_id = prob_id_counter
                    prob_id_counter += 1
                    prob_dict = {
                        "problem_id": problem_id,
                        **{k: task[k] for k in problem_keys},
                    }
                    if prob_dict["difficulty"] == 0:
                        num_removed += 1
                        continue

                    total_tests = 0
                    # Check if test data is available for each test type
                    for t_name in ["public", "private", "generated"]:
                        num_save = 0
                        for ti, to in zip(
                            task[f"{t_name}_tests"]["input"],
                            task[f"{t_name}_tests"]["output"],
                        ):
                            test_file.write(
                                ",".join(
                                    (
                                        str(problem_id),
                                        f"{len(ti)}",
                                        f"{len(to)}",
                                        f"{t_name == 'public'}",
                                        f"{t_name == 'generated'}",
                                        f"{t_name == 'private'}",
                                        f"{to.isnumeric()}",
                                    )
                                )
                                + "\n"
                            )

                            num_save += 1
                            if t_name in {"public", "private"}:
                                total_tests += 1

                            if num_save >= 30:
                                break
                        prob_dict[f"{t_name}_tests"] = len(
                            task.get(f"{t_name}_tests", {"input": []})["input"]
                        )
                    if total_tests == 0:
                        num_removed += 1
                        continue
                    prob_dict["cf_tags"] = []
                    for t in task["cf_tags"]:
                        if t not in TAG_MAPS:
                            TAG_MAPS[t] = len(TAG_MAPS)
                        prob_dict["cf_tags"].append(TAG_MAPS[t])

                    # Extract time limit (if available)
                    prob_dict["time_limit"] = (
                        -1
                        if task["time_limit"] is None
                        else task["time_limit"]["seconds"]
                    )

                    sols = []  # Initialize solutions list (note: not used later)

                    # Process both correct and incorrect solutions
                    for p, sol_dict in [
                        (True, task["solutions"]),  # Correct solutions
                        (False, task["incorrect_solutions"]),  # Incorrect solutions
                    ]:

                        # Group solutions by programming language
                        language_sols = defaultdict(list)
                        for i, sol in enumerate(sol_dict["solution"]):
                            language_sols[sol_dict["language"][i]].append(sol)
                        has_printed = False
                        # For each language, randomly sample a small number of solutions
                        # (to save space, we're not keeping all solutions)
                        for lang, sols in language_sols.items():
                            # Take between 1-3 random solutions per language
                            to_save = random.sample(
                                sols, k=min(len(sols), random.randint(1, 3))
                            )
                            for sol in to_save:
                                # Truncate solutions that are too long
                                if len(sol) > 4096:
                                    sol = sol[:4096] + "...TRUNCATED"
                                save_sol_dict = {
                                    "problem_id": problem_id,
                                    "language": LANGUAGE_MAP[
                                        lang
                                    ],  # Convert language ID to name
                                    "is_correct": p,  # Whether this is a correct solution
                                    "solution": sol,  # The code solution
                                }

                                # Write the solution to the CSV
                                solutions_fd.write(json.dumps(save_sol_dict) + "\n")

                    # Write the problem data to the CSV
                    problems_fd.write(json.dumps(prob_dict) + "\n")

                    problems_saved += 1
                    if problems_saved >= 10_000:
                        break

with Path("nb/data/tags.csv").open("w") as f:
    f.write("tag_id,tag\n")
    for k, v in TAG_MAPS.items():
        if not k:
            continue
        f.write(f"{v},{k}\n")

print(f"Removed {num_removed:,} problems")
