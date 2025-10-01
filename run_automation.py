import subprocess
import logging
import datetime
import os
import sys

# Configuration
# IMPORTANT: Ensure this path is correct for your local setup.
REPO_PATH = r"C:\Users\mikin\projects\NewTeaTrade"

# Determine the Python executable path (e.g., venv)
PYTHON_EXECUTABLE = sys.executable 

# Define the jobs to run sequentially
JOBS_TO_RUN = [
    {"name": "Mombasa Processor (ETL)", "script": "process_mombasa_data.py"},
    {"name": "Mombasa Analyzer (JSON Generation)", "script": "analyze_mombasa.py"},
    # You can add your news scraper here as well
    # {"name": "News Scraper", "script": "scraper_news.py"},
]

# Files/Directories to commit automatically
FILES_TO_COMMIT = [
    "market_reports.db",
    "market-reports.html",
    "report_data/" # Commit the entire data directory
]

# Set up logging to stdout for cron compatibility
logging.basicConfig(level=logging.INFO, format='AUTOMATION: %(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])

# Import GitPython
try:
    import git
except ImportError:
    logging.error("GitPython is required. Please install it: pip install GitPython")
    exit(1)

def run_script(script_name):
    """Executes a Python script."""
    script_path = os.path.join(REPO_PATH, script_name)
    logging.info(f"--- Running {script_name} ---")
    try:
        # Execute the script within the repository directory
        result = subprocess.run(
            [PYTHON_EXECUTABLE, script_path],
            capture_output=True,
            text=True,
            check=True,
            cwd=REPO_PATH
        )
        if result.stdout:
            # Print stdout directly so the logs capture the output from the child scripts
            print(result.stdout.strip())
        logging.info(f"--- Finished {script_name} ---")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running {script_name}:")
        # Log stdout and stderr if the script failed
        if e.stdout:
            logging.error(f"Stdout: {e.stdout}")
        if e.stderr:
            # Decode stderr if it's bytes (common in subprocess errors), otherwise use as is
            stderr_output = e.stderr.decode('utf-8') if isinstance(e.stderr, bytes) else e.stderr
            logging.error(f"Stderr: {stderr_output}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred while running {script_name}: {e}")
        return False

def git_sync_repository():
    """Pulls latest changes, commits the updated files, and pushes to the remote repository."""
    logging.info("--- Starting Git Operations ---")
    try:
        repo = git.Repo(REPO_PATH)
        origin = repo.remote(name='origin')

        # 1. Pull latest changes from remote (Fixes "failed to push some refs")
        logging.info("Pulling latest changes from GitHub (using rebase)...")
        # Using rebase=True to place local commits on top of remote changes cleanly
        origin.pull(rebase=True) 
        
        # 2. Add files/directories
        # We use repo.git.add() as it correctly handles directories and new files
        repo.git.add(FILES_TO_COMMIT)
        
        # 3. Check if there are changes staged (comparing index to HEAD)
        if repo.index.diff('HEAD'):
            # 4. Commit
            commit_message = f"Automated data update: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            repo.index.commit(commit_message)
            logging.info(f"Committed changes.")
            
            # 5. Push
            logging.info("Pushing changes to remote repository...")
            push_info_list = origin.push()
            
            # 6. Verify Push Results
            # Check the results of the push operation
            push_failed = False
            if push_info_list:
                # Check the flags on the first item (assuming push to one branch)
                push_info = push_info_list[0]
                if push_info.flags & git.PushInfo.ERROR:
                    logging.error(f"Push failed: {push_info.summary}")
                    push_failed = True
                elif push_info.flags & git.PushInfo.REJECTED:
                     logging.error(f"Push rejected: {push_info.summary}")
                     push_failed = True
            
            if not push_failed:
                logging.info("Push successful.")

        else:
            logging.info("No changes detected. Skipping commit and push.")

    except git.exc.GitCommandError as e:
        logging.error(f"Git command error: {e}")
        # Log the specific stderr output from the git command if available
        if hasattr(e, 'stderr'):
            error_msg = e.stderr.decode('utf-8') if isinstance(e.stderr, bytes) else str(e.stderr)
            logging.error(f"Git Stderr: {error_msg}")
        logging.error("Ensure Git is installed, the repository (https://github.com/robtaylor1203-cmd/NewTeaTrade) is configured as the remote 'origin', and authentication is set up.")
    except Exception as e:
        logging.error(f"An unexpected error occurred during Git operations: {e}")

def main():
    logging.info("=== Starting Automated Market Data Pipeline (Cron Mode) ===")
    
    all_jobs_successful = True
    for job in JOBS_TO_RUN:
        if not run_script(job['script']):
            logging.error(f"{job['name']} failed. Aborting pipeline.")
            all_jobs_successful = False
            break
    
    if all_jobs_successful:
        git_sync_repository()
    else:
        logging.error("One or more jobs failed. Git repository not updated.")

    logging.info("=== Automated Pipeline Finished ===")

if __name__ == "__main__":
    main()