import logging
import httpx
from huggingface_hub import hf_hub_url

log = logging.getLogger(__name__)


class AsyncHuggingFaceClient:
    def __init__(self, token=None):
        self.token = token
        self.base_url = "https://huggingface.co/api"
        self.headers = {}
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
    async def list_repo_commits(self, repo_id, repo_type = "dataset", limit = 20):
        url = f"{self.base_url}/{repo_type}s/{repo_id}/commits"
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    url,
                    headers=self.headers,
                    params={"limit": limit},
                    timeout=30
                )
                response.raise_for_status()
                commits_data = response.json()
                result = []
                for commit in commits_data[:limit]:
                    commit_dict = {
                        'id': commit.get('id', ''),
                        'title': commit.get('title', ''),
                        'message': commit.get('message', commit.get('title', '')),
                        'author': {
                            'name': commit.get('authors', [{}])[0].get('name', '')
                                   if commit.get('authors') else '',
                            'email': ''
                        },
                        'date': commit.get('date', '')
                    }
                    result.append(commit_dict)
                return result
            except httpx.HTTPError as exc:
                log.error(f'Error fetching commits for {repo_id}: {exc}')
                raise
            except Exception as exc:
                log.error(f'Unexpected error fetching commits for {repo_id}: {exc}')
                raise

    async def list_repo_files(self, repo_id, repo_type = "dataset", revision=None):
        url = f"{self.base_url}/{repo_type}s/{repo_id}/tree/{revision or 'main'}"
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    url,
                    headers=self.headers,
                    timeout=30
                )
                response.raise_for_status()
                tree_data = response.json()
                files = []
                for item in tree_data:
                    if item.get('type') == 'file':
                        files.append(item.get('path', ''))
                return files
            except httpx.HTTPError as exc:
                log.error(f'Error listing files for {repo_id}: {exc}')
                raise
            except Exception as exc:
                log.error(f'Unexpected error listing files for {repo_id}: {exc}')
                raise

    def get_file_url(self, repo_id, filename, repo_type = "dataset", revision=None):
        return hf_hub_url(
            repo_id=repo_id,
            filename=filename,
            repo_type=repo_type,
            revision=revision
        )