"""HTTP utilities for DeFi BI-ETL pipeline."""

import time
from typing import Any, Dict, Optional

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(
        (requests.exceptions.RequestException, requests.exceptions.HTTPError)
    ),
)
def get_json(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
) -> Any:
    """
    Generic GET request with retries and exponential backoff.

    Args:
        url: Target URL
        headers: Optional headers
        params: Optional query parameters
        timeout: Request timeout in seconds

    Returns:
        JSON response as dictionary

    Raises:
        requests.exceptions.RequestException: On request failure
        requests.exceptions.HTTPError: On HTTP error (4xx, 5xx)
    """
    try:
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        response.raise_for_status()

        # Handle rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            print(f"Rate limited. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
            raise requests.exceptions.HTTPError("Rate limited")

        return response.json()

    except requests.exceptions.HTTPError as e:
        if e.response.status_code in [429, 500, 502, 503, 504]:
            print(f"HTTP {e.response.status_code}: {e.response.text}")
            raise
        else:
            print(f"HTTP Error {e.response.status_code}: {e.response.text}")
            raise
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        raise


def get_with_retry(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = 5,
    timeout: int = 30,
) -> Any:
    """
    Alternative GET function with manual retry logic.

    Args:
        url: Target URL
        headers: Optional headers
        params: Optional query parameters
        max_retries: Maximum number of retry attempts
        timeout: Request timeout in seconds

    Returns:
        JSON response as dictionary
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url, headers=headers, params=params, timeout=timeout
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get("Retry-After", 60))
                print(
                    f"Rate limited (attempt {attempt + 1}/{max_retries}). Waiting {retry_after}s..."
                )
                time.sleep(retry_after)
            elif e.response.status_code >= 500:
                wait_time = 2**attempt
                print(
                    f"Server error (attempt {attempt + 1}/{max_retries}). Waiting {wait_time}s..."
                )
                time.sleep(wait_time)
            else:
                print(f"HTTP Error {e.response.status_code}: {e.response.text}")
                raise

        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                print(f"Request failed after {max_retries} attempts: {e}")
                raise
            wait_time = 2**attempt
            print(
                f"Request failed (attempt {attempt + 1}/{max_retries}). Waiting {wait_time}s..."
            )
            time.sleep(wait_time)

    raise requests.exceptions.RequestException(f"Failed after {max_retries} attempts")
