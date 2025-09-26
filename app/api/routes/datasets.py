import logging
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import JSONResponse
from app.core.config import settings
from app.integrations.hf_datasets import get_dataset_commits_async, get_dataset_files_async, get_datasets_page_from_cache, get_file_url_async
from app.integrations.redis_client import cache_get
router = APIRouter(prefix='/datasets', tags=['datasets'])
log = logging.getLogger(__name__)

@router.get('/cache-status')
async def cache_status():
    meta = await cache_get('hf:datasets:meta')
    last_update = meta.get('last_update') if isinstance(meta, dict) else None
    total_items = int(meta.get('total_items', 0)) if isinstance(meta, dict) else 0
    refreshing = bool(meta.get('refreshing')) if isinstance(meta, dict) else False
    warming_up = total_items == 0
    return {
        'last_update': last_update,
        'total_items': total_items,
        'warming_up': warming_up,
        'refreshing': refreshing,
    }

@router.get('/')
async def list_datasets(request: Request):
    params = request.query_params
    try:
        limit = int(params.get('limit', 10))
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail='Invalid limit value')
    if limit < 1 or limit > 1000:
        raise HTTPException(status_code=422, detail='Invalid limit value')
    try:
        offset = int(params.get('offset', 0))
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail='Invalid offset value')
    if offset < 0:
        raise HTTPException(status_code=422, detail='Invalid offset value')
    search = params.get('search')
    if search == '':
        search = None
    sort_by = params.get('sort_by')
    if sort_by == '':
        sort_by = None
    sort_order = params.get('sort_order', 'desc')
    if sort_order not in ('asc', 'desc'):
        raise HTTPException(status_code=422, detail='Invalid sort order')
    (result, status_code) = get_datasets_page_from_cache(limit, offset, search, sort_by, sort_order)
    if status_code != 200:
        return JSONResponse(result, status_code=status_code)
    return result

@router.get('/{dataset_id:path}/commits')
async def get_commits(dataset_id):
    try:
        return await get_dataset_commits_async(dataset_id)
    except Exception as exc:
        log.error('Error fetching commits for %s: %s', dataset_id, exc)
        raise HTTPException(status_code=404, detail=f'Could not fetch commits: {exc}') from exc

@router.get('/{dataset_id:path}/files')
async def list_files(dataset_id):
    try:
        return await get_dataset_files_async(dataset_id)
    except Exception as exc:
        log.error('Error listing files for %s: %s', dataset_id, exc)
        raise HTTPException(status_code=404, detail=f'Could not list files: {exc}') from exc

@router.get('/{dataset_id:path}/file-url')
async def get_file_url_endpoint(dataset_id, request: Request):
    filename = request.query_params.get('filename')
    if not filename:
        raise HTTPException(status_code=422, detail='filename is required')
    revision = request.query_params.get('revision')
    url = await get_file_url_async(dataset_id, filename, revision)
    return {'download_url': url}

@router.get('/meta')
async def get_datasets_meta():
    meta = await cache_get('hf:datasets:meta')
    return meta if isinstance(meta, dict) else {}

@router.post('/refresh-cache', status_code=status.HTTP_202_ACCEPTED)
async def refresh_cache():
    token = settings.resolve_hf_token()
    if not token:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail='Hugging Face token not configured')
    from app.tasks.dataset_tasks import refresh_hf_datasets_full_cache
    task = refresh_hf_datasets_full_cache.delay()
    return {'status': 'accepted', 'task_id': task.id}
