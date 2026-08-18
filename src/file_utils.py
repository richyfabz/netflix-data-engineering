"""
File utilities for the Netflix data engineering pipeline.

This module contains reusable helpers for working with source files,
including checksum generation for duplicate-batch detection.
"""

# Import hashlib so we can generate cryptographic file fingerprints.
import hashlib


# Define a reusable function for generating a SHA-256 checksum.
def calculate_file_checksum(file_path):
    """
    Return the SHA-256 checksum of a file.

    The file is read in chunks so large source files do not need
    to be loaded completely into memory.
    """

    # Create an empty SHA-256 hashing object.
    sha256 = hashlib.sha256()

    # Open the source file in binary mode.
    with open(file_path, "rb") as source_file:

        # Continue reading until the end of the file.
        while True:

            # Read approximately 1 MB of data at a time.
            chunk = source_file.read(1024 * 1024)

            # Stop once there are no more bytes to read.
            if not chunk:
                break

            # Add this chunk to the running hash calculation.
            sha256.update(chunk)

    # Return the final checksum as readable hexadecimal text.
    return sha256.hexdigest()