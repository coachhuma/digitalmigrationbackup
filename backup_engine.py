import json
import zipfile
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, List
from file_operations import FileOperations   # reuse your existing helper
from notification_system import NotificationManager
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

@dataclass
class BackupStats:
    total_files: int
    total_size: int
    total_time: float
    files_skipped: int
    errors: int
    start_time: str
    end_time: str

    def to_dict(self):
        return asdict(self)


class BackupEngine:
    """Core backup functionality with incremental + compression support."""

    def __init__(self, notification_manager: NotificationManager):
        self.notification_manager = notification_manager
        self.stats: Optional[BackupStats] = None

    def backup_directory(
        self,
        source: str,
        destination: str,
        incremental: bool = False,
        compression: bool = False,
        exclude_patterns: Optional[List[str]] = None
    ) -> bool:
        start_time = datetime.utcnow()
        source_path = Path(source)
        index_file = Path(destination) / "backup_index.json"
        previous_index = {}

        # Load previous index if incremental
        if incremental and index_file.exists():
            try:
                with open(index_file, 'r') as f:
                    previous_index = json.load(f)
            except Exception as e:
                logger.warning(f"Could not load previous index: {e}")

        total_files, total_size, files_skipped, errors = 0, 0, 0, 0

        try:
            for file_path in source_path.rglob('*'):
                if file_path.is_file():
                    rel_path = str(file_path.relative_to(source_path))
                    metadata = FileOperations.get_file_metadata(str(file_path))

                    # Skip unchanged files if incremental
                    if incremental and rel_path in previous_index:
                        prev_meta = previous_index[rel_path]
                        if prev_meta["checksum"] == metadata.checksum and \
                           prev_meta["modified_time"] == metadata.modified_time:
                            files_skipped += 1
                            continue

                    dest_file = Path(destination) / rel_path
                    if FileOperations.copy_file(str(file_path), str(dest_file)):
                        total_files += 1
                        total_size += file_path.stat().st_size
                        previous_index[rel_path] = metadata.to_dict()
                    else:
                        errors += 1

            # Save updated index
            if incremental:
                try:
                    with open(index_file, 'w') as f:
                        json.dump(previous_index, f, indent=2)
                except Exception as e:
                    logger.error(f"Failed to save backup index: {e}")

            # Compression logic
            if compression:
                try:
                    zip_name = Path(destination) / "backup_archive.zip"
                    with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
                        for rel_path, meta in previous_index.items():
                            file_path = Path(source) / rel_path
                            if file_path.exists():
                                zipf.write(file_path, arcname=rel_path)
                    self.notification_manager.notify(
                        "Compression Completed",
                        f"Backup compressed into {zip_name}",
                        "success"
                    )
                except Exception as e:
                    logger.error(f"Compression failed: {e}")
                    self.notification_manager.notify(
                        "Compression Error",
                        f"Failed to compress backup: {str(e)}",
                        "error"
                    )

            # Stats + notification
            end_time = datetime.utcnow()
            elapsed_time = (end_time - start_time).total_seconds()
            self.stats = BackupStats(
                total_files=total_files,
                total_size=total_size,
                total_time=elapsed_time,
                files_skipped=files_skipped,
                errors=errors,
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat()
            )
            self.notification_manager.notify(
                "Backup Completed",
                f"Backed up {total_files} files ({self._format_size(total_size)}) "
                f"in {elapsed_time:.2f}s",
                "success"
            )
            return True

        except Exception as e:
            logger.error(f"Backup failed: {e}")
            self.notification_manager.notify(
                "Backup Error",
                f"An error occurred during backup: {str(e)}",
                "error"
            )
            return False

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} PB"
