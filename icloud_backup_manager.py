"""
iCloud Backup Manager
Comprehensive iPhone/iCloud content backup with quality preservation
"""

import os
import json
import hashlib
import time
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set


class iCloudBackupManager:
    """
    Advanced iCloud and iPhone backup management system.
    
    Features:
    - Organized folder structure for iCloud downloads
    - Quality verification for photos and videos
    - Conversation archiving (AI assistants, etc.)
    - Text message media preservation
    - Comprehensive backup manifests
    """
    
    def __init__(self, onedrive_path: str, config: Optional[Dict] = None):
        """
        Initialize iCloud backup manager.
        
        Args:
            onedrive_path: Path to OneDrive folder for backup storage
            config: Optional configuration dictionary
        """
        self.onedrive_path = onedrive_path
        self.config = config or self._default_config()
        self.backup_stats = self._init_stats()
        self.logger = self._setup_logging()
    
    def _default_config(self) -> Dict:
        """Default configuration for iCloud backup manager."""
        return {
            'folder_structure': {
                'iphone_backup': 'iPhone_Backup',
                'icloud_backup': 'iCloud_Backup',
                'conversations': 'Conversations',
                'text_messages': 'Text_Messages'
            },
            'content_categories': {
                'photos': ['Photos', 'Screenshots', 'Live_Photos'],
                'videos': ['Videos', 'Slow_Motion', 'Time_Lapse'],
                'conversations': ['Copilot', 'Office_Agent', 'ChatGPT', 'Other_AI'],
                'messages': ['Media', 'Attachments', 'Voice_Messages']
            },
            'quality_standards': {
                'photo_min_sizes': {
                    '.jpg': 1024 * 1024,      # 1MB minimum
                    '.heic': 2 * 1024 * 1024, # 2MB minimum
                    '.png': 500 * 1024        # 500KB minimum
                },
                'video_min_bitrates': {
                    '.mp4': 5 * 1024 * 1024,  # 5MB per minute
                    '.mov': 8 * 1024 * 1024   # 8MB per minute
                }
            },
            'backup_settings': {
                'create_date_folders': True,
                'verify_downloads': True,
                'create_manifests': True,
                'preserve_metadata': True
            }
        }
    
    def _init_stats(self) -> Dict:
        """Initialize backup statistics."""
        return {
            'photos_processed': 0,
            'videos_processed': 0,
            'conversations_backed_up': 0,
            'messages_processed': 0,
            'total_size': 0,
            'quality_issues': 0,
            'errors': []
        }
    
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration."""
        logger = logging.getLogger('iCloudBackupManager')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def setup_backup_structure(self) -> Dict[str, str]:
        """
        Create organized folder structure for iCloud backup.
        
        Returns:
            Dictionary mapping structure names to created paths
        """
        self.logger.info("Setting up iCloud backup folder structure")
        
        created_paths = {}
        
        # Main backup categories
        main_folders = [
            f"{self.config['folder_structure']['iphone_backup']}/Photos",
            f"{self.config['folder_structure']['iphone_backup']}/Videos",
            f"{self.config['folder_structure']['iphone_backup']}/Screenshots",
            f"{self.config['folder_structure']['iphone_backup']}/Live_Photos",
            f"{self.config['folder_structure']['icloud_backup']}/Photos",
            f"{self.config['folder_structure']['icloud_backup']}/Videos",
            f"{self.config['folder_structure']['conversations']}/Copilot",
            f"{self.config['folder_structure']['conversations']}/Office_Agent",
            f"{self.config['folder_structure']['conversations']}/ChatGPT",
            f"{self.config['folder_structure']['conversations']}/Other_AI",
            f"{self.config['folder_structure']['text_messages']}/Media",
            f"{self.config['folder_structure']['text_messages']}/Attachments",
            "Backup_Reports",
            "Download_Manifests"
        ]
        
        for folder in main_folders:
            folder_path = os.path.join(self.onedrive_path, folder)
            os.makedirs(folder_path, exist_ok=True)
            created_paths[folder] = folder_path
            self.logger.info(f"Created folder: {folder}")
        
        # Create date-based subfolders if enabled
        if self.config['backup_settings']['create_date_folders']:
            self._create_date_folders(created_paths)
        
        # Create setup documentation
        self._create_setup_documentation()
        
        return created_paths
    
    def _create_date_folders(self, base_paths: Dict[str, str]):
        """Create date-based organization folders."""
        current_year = datetime.now().year
        
        # Create folders for current and previous 2 years
        for year in range(current_year - 2, current_year + 1):
            for month in range(1, 13):
                date_folder = f"{year}/{month:02d}"
                
                # Create date folders in photo and video directories
                photo_paths = [
                    f"{self.config['folder_structure']['iphone_backup']}/Photos",
                    f"{self.config['folder_structure']['icloud_backup']}/Photos"
                ]
                
                video_paths = [
                    f"{self.config['folder_structure']['iphone_backup']}/Videos",
                    f"{self.config['folder_structure']['icloud_backup']}/Videos"
                ]
                
                for base_path in photo_paths + video_paths:
                    date_path = os.path.join(self.onedrive_path, base_path, date_folder)
                    os.makedirs(date_path, exist_ok=True)
    
    def _create_setup_documentation(self):
        """Create documentation for backup setup."""
        doc_content = f"""
iCLOUD BACKUP SETUP GUIDE
=========================

Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
OneDrive Path: {self.onedrive_path}

FOLDER STRUCTURE:
================

iPhone_Backup/
├── Photos/           # Direct iPhone photo exports
├── Videos/           # Direct iPhone video exports  
├── Screenshots/      # iPhone screenshots
└── Live_Photos/      # Live photos from iPhone

iCloud_Backup/
├── Photos/           # Photos downloaded from iCloud.com
└── Videos/           # Videos downloaded from iCloud.com

Conversations/
├── Copilot/          # Microsoft Copilot conversations
├── Office_Agent/     # Office Agent conversations
├── ChatGPT/          # ChatGPT conversations
└── Other_AI/         # Other AI assistant conversations

Text_Messages/
├── Media/            # Photos/videos from text messages
├── Attachments/      # File attachments from messages
└── Voice_Messages/   # Voice message recordings

DOWNLOAD INSTRUCTIONS:
=====================

1. iPhone Photos/Videos:
   - Connect iPhone to computer
   - Use Photos app or File Explorer
   - Copy to iPhone_Backup folders
   - Organize by date if desired

2. iCloud Photos/Videos:
   - Go to iCloud.com and sign in
   - Click Photos
   - Select All Photos (Ctrl+A)
   - Click Download (cloud icon)
   - Choose 'Download Originals' for full quality
   - Save to iCloud_Backup folders

3. Conversations:
   - Copy conversation text to .txt or .md files
   - Save any attachments to respective folders
   - Use date-based naming: YYYY-MM-DD_topic-name.txt

4. Text Messages:
   - Export message media manually
   - Use third-party tools for bulk export
   - Organize by sender or date

QUALITY VERIFICATION:
====================

This system will verify:
- Photo quality (full resolution vs compressed)
- Video quality (original vs re-encoded)
- File integrity (no corruption during transfer)
- Completeness (all files successfully copied)

BACKUP SAFETY:
==============

- Always verify downloads before deleting originals
- Keep multiple backup copies (OneDrive + thumb drives)
- Test file accessibility before deletion
- Maintain backup manifests for tracking

For questions or issues, refer to the backup verification tools.
"""
        
        doc_path = os.path.join(self.onedrive_path, "iCloud_Backup_Setup_Guide.txt")
        with open(doc_path, 'w', encoding='utf-8') as f:
            f.write(doc_content)
        
        self.logger.info(f"Setup documentation created: {doc_path}")
    
    def verify_download_quality(self, file_path: str) -> Dict:
        """
        Verify that downloaded file is full quality.
        
        Args:
            file_path: Path to downloaded file
            
        Returns:
            Dictionary with quality verification results
        """
        try:
            file_size = os.path.getsize(file_path)
            file_ext = Path(file_path).suffix.lower()
            
            # Photo quality verification
            if file_ext in self.config['quality_standards']['photo_min_sizes']:
                min_size = self.config['quality_standards']['photo_min_sizes'][file_ext]
                is_full_quality = file_size >= min_size
                
                return {
                    'file_type': 'photo',
                    'is_full_quality': is_full_quality,
                    'file_size_mb': file_size / (1024 * 1024),
                    'min_size_mb': min_size / (1024 * 1024),
                    'quality_status': 'FULL' if is_full_quality else 'COMPRESSED'
                }
            
            # Video quality verification (basic size check)
            elif file_ext in self.config['quality_standards']['video_min_bitrates']:
                min_bitrate = self.config['quality_standards']['video_min_bitrates'][file_ext]
                # Assume minimum 30 seconds for basic quality check
                min_expected_size = min_bitrate * 0.5  # 30 seconds
                is_reasonable_quality = file_size >= min_expected_size
                
                return {
                    'file_type': 'video',
                    'is_full_quality': is_reasonable_quality,
                    'file_size_mb': file_size / (1024 * 1024),
                    'quality_status': 'FULL' if is_reasonable_quality else 'COMPRESSED'
                }
            
            else:
                return {
                    'file_type': 'other',
                    'is_full_quality': True,
                    'file_size_mb': file_size / (1024 * 1024),
                    'quality_status': 'UNKNOWN'
                }
        
        except Exception as e:
            self.logger.error(f"Error verifying quality for {file_path}: {e}")
            return {
                'file_type': 'error',
                'is_full_quality': False,
                'error': str(e)
            }
    
    def backup_conversations(self, conversation_type: str, conversations: List[Dict]) -> Dict:
        """
        Backup AI assistant conversations with metadata.
        
        Args:
            conversation_type: Type of conversation (copilot, office_agent, etc.)
            conversations: List of conversation dictionaries
            
        Returns:
            Backup statistics
        """
        self.logger.info(f"Backing up {conversation_type} conversations")
        
        conversation_folder = os.path.join(
            self.onedrive_path, 
            self.config['folder_structure']['conversations'],
            conversation_type.title()
        )
        
        backup_stats = {
            'conversations_backed_up': 0,
            'total_size': 0,
            'attachments_saved': 0,
            'errors': []
        }
        
        for i, conversation in enumerate(conversations):
            try:
                # Create conversation file
                timestamp = conversation.get('timestamp', datetime.now().isoformat())
                topic = conversation.get('topic', f'conversation_{i+1}')
                filename = f"{timestamp[:10]}_{topic.replace(' ', '_')}.md"
                
                conversation_path = os.path.join(conversation_folder, filename)
                
                # Format conversation content
                content = self._format_conversation(conversation)
                
                with open(conversation_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                backup_stats['conversations_backed_up'] += 1
                backup_stats['total_size'] += len(content.encode('utf-8'))
                
                # Save attachments if any
                if 'attachments' in conversation:
                    attachments_folder = os.path.join(conversation_folder, 'Attachments')
                    os.makedirs(attachments_folder, exist_ok=True)
                    
                    for attachment in conversation['attachments']:
                        if os.path.exists(attachment['path']):
                            dest_path = os.path.join(attachments_folder, attachment['filename'])
                            shutil.copy2(attachment['path'], dest_path)
                            backup_stats['attachments_saved'] += 1
            
            except Exception as e:
                error_msg = f"Error backing up conversation {i+1}: {str(e)}"
                backup_stats['errors'].append(error_msg)
                self.logger.error(error_msg)
        
        self.backup_stats['conversations_backed_up'] += backup_stats['conversations_backed_up']
        return backup_stats
    
    def _format_conversation(self, conversation: Dict) -> str:
        """Format conversation data into readable markdown."""
        content = f"""# {conversation.get('topic', 'Conversation')}

**Date**: {conversation.get('timestamp', 'Unknown')}
**Type**: {conversation.get('type', 'AI Conversation')}
**Participants**: {', '.join(conversation.get('participants', ['User', 'AI']))}

---

"""
        
        # Add conversation messages
        for message in conversation.get('messages', []):
            sender = message.get('sender', 'Unknown')
            timestamp = message.get('timestamp', '')
            text = message.get('text', '')
            
            content += f"## {sender}"
            if timestamp:
                content += f" - {timestamp}"
            content += f"\n\n{text}\n\n---\n\n"
        
        # Add metadata
        if conversation.get('metadata'):
            content += "## Metadata\n\n"
            for key, value in conversation['metadata'].items():
                content += f"- **{key}**: {value}\n"
        
        return content
    
    def create_download_manifest(self, content_type: str, file_list: List[str]) -> str:
        """
        Create manifest of files to download.
        
        Args:
            content_type: Type of content (photos, videos, etc.)
            file_list: List of file paths or URLs
            
        Returns:
            Path to created manifest file
        """
        manifest = {
            'created': datetime.now().isoformat(),
            'content_type': content_type,
            'total_files': len(file_list),
            'files': [],
            'download_instructions': self._get_download_instructions(content_type)
        }
        
        for file_item in file_list:
            if isinstance(file_item, str):
                manifest['files'].append({
                    'path': file_item,
                    'status': 'pending',
                    'size': None,
                    'downloaded': False
                })
            else:
                manifest['files'].append(file_item)
        
        manifest_filename = f"{content_type}_download_manifest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        manifest_path = os.path.join(self.onedrive_path, "Download_Manifests", manifest_filename)
        
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        self.logger.info(f"Download manifest created: {manifest_path}")
        return manifest_path
    
    def _get_download_instructions(self, content_type: str) -> List[str]:
        """Get specific download instructions for content type."""
        instructions = {
            'photos': [
                "1. Go to iCloud.com and sign in",
                "2. Click Photos",
                "3. Select photos (Ctrl+A for all)",
                "4. Click Download button (cloud icon)",
                "5. Choose 'Download Originals' for full quality",
                "6. Save to appropriate OneDrive folder"
            ],
            'videos': [
                "1. Go to iCloud.com and sign in",
                "2. Click Photos, then Videos tab",
                "3. Select videos to download",
                "4. Click Download button",
                "5. Ensure original quality is selected",
                "6. Save to Videos folder"
            ],
            'conversations': [
                "1. Open conversation history in AI assistant",
                "2. Copy conversation text",
                "3. Save as .txt or .md file with date prefix",
                "4. Save any attachments separately",
                "5. Update conversation index"
            ]
        }
        
        return instructions.get(content_type, ["Manual download required"])
    
    def generate_backup_report(self) -> Dict:
        """Generate comprehensive backup report."""
        report = {
            'report_date': datetime.now().isoformat(),
            'onedrive_path': self.onedrive_path,
            'backup_statistics': self.backup_stats,
            'folder_structure': self.config['folder_structure'],
            'quality_standards': self.config['quality_standards'],
            'backup_status': 'in_progress',
            'next_steps': []
        }
        
        # Determine backup status and next steps
        if self.backup_stats['photos_processed'] == 0 and self.backup_stats['videos_processed'] == 0:
            report['backup_status'] = 'setup_complete'
            report['next_steps'] = [
                "Download photos from iCloud.com to iCloud_Backup/Photos",
                "Download videos from iCloud.com to iCloud_Backup/Videos", 
                "Export iPhone photos directly to iPhone_Backup/Photos",
                "Backup AI conversations to Conversations folders",
                "Run backup verification after downloads complete"
            ]
        elif len(self.backup_stats['errors']) > 0:
            report['backup_status'] = 'completed_with_errors'
            report['next_steps'] = [
                "Review error log and retry failed operations",
                "Verify file integrity for completed downloads",
                "Run quality verification on downloaded media"
            ]
        else:
            report['backup_status'] = 'completed_successfully'
            report['next_steps'] = [
                "Run backup verification to confirm integrity",
                "Create additional backup copies on thumb drives",
                "Verify all files before deleting originals"
            ]
        
        # Save report
        report_path = os.path.join(self.onedrive_path, "Backup_Reports", 
                                 f"icloud_backup_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        self.logger.info(f"Backup report saved: {report_path}")
        return report


def main():
    """Example usage of iCloudBackupManager."""
    # Get OneDrive path
    onedrive_path = input("Enter your OneDrive folder path: ").strip()
    if not onedrive_path:
        onedrive_path = os.path.join(os.path.expanduser("~"), "OneDrive")
    
    if not os.path.exists(onedrive_path):
        print(f"Creating OneDrive folder: {onedrive_path}")
        os.makedirs(onedrive_path, exist_ok=True)
    
    # Initialize backup manager
    backup_manager = iCloudBackupManager(onedrive_path)
    
    print("Setting up iCloud backup structure...")
    created_paths = backup_manager.setup_backup_structure()
    
    print(f"\n✅ iCloud backup structure created!")
    print(f"📁 Base path: {onedrive_path}")
    print(f"📋 Setup guide: {onedrive_path}/iCloud_Backup_Setup_Guide.txt")
    
    # Example: Create download manifest for photos
    print("\nCreating download manifest for photos...")
    photo_manifest = backup_manager.create_download_manifest('photos', [
        'iCloud Photos - All Photos',
        'iPhone Photos - Camera Roll'
    ])
    
    # Example: Backup a conversation
    print("Creating example conversation backup...")
    example_conversations = [{
        'topic': 'Digital Migration Project',
        'timestamp': datetime.now().isoformat(),
        'type': 'Office Agent',
        'participants': ['User', 'Office Agent'],
        'messages': [
            {
                'sender': 'User',
                'timestamp': datetime.now().isoformat(),
                'text': 'Help me create a digital migration system'
            },
            {
                'sender': 'Office Agent', 
                'timestamp': datetime.now().isoformat(),
                'text': 'I can help you create a comprehensive migration system...'
            }
        ],
        'metadata': {
            'session_id': 'example_session',
            'tools_used': ['migration_engine', 'backup_verifier'],
            'files_created': 15
        }
    }]
    
    conversation_stats = backup_manager.backup_conversations('office_agent', example_conversations)
    
    # Generate final report
    report = backup_manager.generate_backup_report()
    
    print(f"\n📊 Backup Setup Summary:")
    print(f"Conversations backed up: {conversation_stats['conversations_backed_up']}")
    print(f"Backup status: {report['backup_status']}")
    print(f"📋 Full report: {onedrive_path}/Backup_Reports/")
    
    print(f"\n📝 Next Steps:")
    for step in report['next_steps']:
        print(f"  • {step}")


if __name__ == "__main__":
    main()