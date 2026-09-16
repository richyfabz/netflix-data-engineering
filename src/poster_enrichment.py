import os
import requests
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv("../.env")

TMDB_BASE_URL = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500"
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

def fetch_tmdb_metadata(imdb_id: str, token: str) -> dict:
    """Find a movie/show on TMDB using its IMDb ID."""

    url = f"{TMDB_BASE_URL}/find/{imdb_id}"

    response = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "accept": "application/json",
        },
        params={"external_source": "imdb_id"},
        timeout=30,
    )

    response.raise_for_status()
    data = response.json()

    matches = data.get("movie_results", []) or data.get("tv_results", [])

    if not matches:
        return {
            "tmdb_id": None,
            "tmdb_title": None,
            "poster_path": None,
            "poster_url": None,
            "match_status": "NOT_FOUND",
        }

    match = matches[0]
    poster_path = match.get("poster_path")

    return {
        "tmdb_id": match.get("id"),
        "tmdb_title": match.get("title") or match.get("name"),
        "poster_path": poster_path,
        "poster_url": (
            f"{TMDB_IMAGE_BASE_URL}{poster_path}"
            if poster_path
            else None
        ),
        "match_status": "MATCHED",
    }

def get_titles_to_enrich(conn, limit=None):
    """
    Return OLAP titles that do not yet have an enrichment record.

    IMDb ID comes from the OLTP layer while title_sk comes
    from the dimensional layer.
    """

    sql = """
        SELECT
            d.title_sk,
            o.imdb_id,
            d.title_name,
            d.type,
            d.release_year
        FROM uat_olap.dim_title AS d
        JOIN uat_oltp.title AS o
            ON o.title_id = d.title_id
        LEFT JOIN uat_olap.title_enrichment AS e
            ON e.title_sk = d.title_sk
        WHERE e.title_sk IS NULL
          AND o.imdb_id IS NOT NULL
          AND BTRIM(o.imdb_id) <> ''
        ORDER BY d.title_sk
    """

    params = ()

    if limit is not None:
        sql += " LIMIT %s"
        params = (limit,)

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def upsert_enrichment(conn, title_sk, imdb_id, metadata):
    sql = """
        INSERT INTO uat_olap.title_enrichment (
            title_sk,
            imdb_id,
            tmdb_id,
            tmdb_title,
            poster_path,
            poster_url,
            match_status,
            enriched_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (title_sk)
        DO UPDATE SET
            imdb_id = EXCLUDED.imdb_id,
            tmdb_id = EXCLUDED.tmdb_id,
            tmdb_title = EXCLUDED.tmdb_title,
            poster_path = EXCLUDED.poster_path,
            poster_url = EXCLUDED.poster_url,
            match_status = EXCLUDED.match_status,
            enriched_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                title_sk,
                imdb_id,
                metadata["tmdb_id"],
                metadata["tmdb_title"],
                metadata["poster_path"],
                metadata["poster_url"],
                metadata["match_status"],
            ),
        )
def search_tmdb_fallback(title, content_type, release_year):
    """
    Search TMDB by title when the IMDb-ID lookup cannot find a title.

    Returns the best validated TMDB result or None.
    """

    media_type = "tv" if content_type == "SHOW" else "movie"

    params = {
        "api_key": TMDB_API_KEY,
        "query": title,
    }

    # TMDB uses different year parameters for movies and TV shows.
    if release_year:
        if media_type == "movie":
            params["year"] = release_year
        else:
            params["first_air_date_year"] = release_year

    response = requests.get(
        f"{TMDB_BASE_URL}/search/{media_type}",
        params=params,
        timeout=30,
    )
    response.raise_for_status()

    results = response.json().get("results", [])

    if not results:
        return None

    # Validate candidates instead of blindly accepting results[0].
    source_title = title.strip().casefold()

    for result in results:
        tmdb_title = (
            result.get("name")
            if media_type == "tv"
            else result.get("title")
        )

        if not tmdb_title:
            continue

        if tmdb_title.strip().casefold() != source_title:
            continue

        date_value = (
            result.get("first_air_date")
            if media_type == "tv"
            else result.get("release_date")
        )

        # If TMDB provides a year, make sure it matches our source year.
        if release_year and date_value:
            try:
                tmdb_year = int(date_value[:4])
                if tmdb_year != int(release_year):
                    continue
            except (ValueError, TypeError):
                continue

        return result

    return None

def get_not_found_titles(conn, limit=None):
    """
    Return titles that failed the primary IMDb-ID TMDB lookup.

    These rows are candidates for the secondary
    title + type + release-year search.
    """

    sql = """
        SELECT
            d.title_sk,
            e.imdb_id,
            d.title_name,
            d.type,
            d.release_year
        FROM uat_olap.title_enrichment AS e
        JOIN uat_olap.dim_title AS d
            ON d.title_sk = e.title_sk
        WHERE e.match_status = 'NOT_FOUND'
        ORDER BY d.title_sk
    """

    params = ()

    if limit is not None:
        sql += " LIMIT %s"
        params = (limit,)

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def enrich_missing_posters(conn, limit=None):
    """
    Retry titles that failed IMDb-ID matching using a validated
    title + content type + release-year TMDB search.
    """

    if not TMDB_API_KEY:
        raise RuntimeError("TMDB_API_KEY is not configured")

    titles = get_not_found_titles(conn, limit=limit)

    stats = {
        "processed": 0,
        "fallback_matched": 0,
        "still_not_found": 0,
        "errors": 0,
    }

    for row in titles:
        title_sk = row["title_sk"]
        imdb_id = row["imdb_id"]
        title_name = row["title_name"]
        content_type = row["type"]
        release_year = row["release_year"]

        try:
            match = search_tmdb_fallback(
                title_name,
                content_type,
                release_year,
            )

            stats["processed"] += 1

            if match:
                poster_path = match.get("poster_path")

                metadata = {
                    "tmdb_id": match.get("id"),
                    "tmdb_title": (
                        match.get("name")
                        if content_type == "SHOW"
                        else match.get("title")
                    ),
                    "poster_path": poster_path,
                    "poster_url": (
                        f"{TMDB_IMAGE_BASE_URL}{poster_path}"
                        if poster_path
                        else None
                    ),
                    "match_status": "FALLBACK_MATCHED",
                }

                upsert_enrichment(
                    conn,
                    title_sk,
                    imdb_id,
                    metadata,
                )

                stats["fallback_matched"] += 1

            else:
                stats["still_not_found"] += 1

        except requests.RequestException as exc:
            stats["errors"] += 1
            print(
                f"TMDB fallback failed for "
                f"{title_name} ({release_year}): {exc}"
            )

    conn.commit()

    return stats

def enrich_posters(conn, limit=None):
    token = os.getenv("TMDB_READ_ACCESS_TOKEN")

    if not token:
        raise RuntimeError("TMDB_READ_ACCESS_TOKEN is not configured")

    titles = get_titles_to_enrich(conn, limit=limit)

    stats = {
        "processed": 0,
        "matched": 0,
        "not_found": 0,
        "errors": 0,
    }

    for row in titles:
        title_sk = row["title_sk"]
        imdb_id = row["imdb_id"]

        try:
            metadata = fetch_tmdb_metadata(imdb_id, token)
            upsert_enrichment(conn, title_sk, imdb_id, metadata)

            stats["processed"] += 1

            if metadata["match_status"] == "MATCHED":
                stats["matched"] += 1
            else:
                stats["not_found"] += 1

        except requests.RequestException as exc:
            stats["errors"] += 1
            print(f"TMDB request failed for {imdb_id}: {exc}")

    conn.commit()

    return stats

