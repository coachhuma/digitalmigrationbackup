"""
Beautiful Flower-Shaped GUI for File Migration and Backup
Created: 2025-12-08
Theme: Orange-Pink with elegant flower design
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import shutil
import os
from pathlib import Path
from datetime import datetime
import threading


class FlowerMigrationApp:
    """Flower-shaped GUI application for file migration and backup"""
    
    # Orange-Pink Theme Colors
    COLORS = {
        'primary_orange': '#FF6B35',      # Vibrant Orange
        'light_orange': '#FFB4A2',        # Light Orange
        'deep_pink': '#E76F51',           # Deep Pink
        'rose_pink': '#F4A582',           # Rose Pink
        'cream': '#FFF8F3',               # Cream background
        'dark_text': '#3D2817',           # Dark brown for text
        'accent': '#FF8C42',              # Accent Orange
    }
    
    def __init__(self, root):
        self.root = root
        self.root.title("🌸 Digital Migration & Backup")
        self.root.geometry("900x1000")
        self.root.configure(bg=self.COLORS['cream'])
        
        # Configure style
        self.setup_styles()
        
        # State variables
        self.source_path = tk.StringVar()
        self.destination_path = tk.StringVar()
        self.is_running = False
        self.progress_value = tk.DoubleVar()
        
        # Build UI
        self.create_flower_ui()
        
    def setup_styles(self):
        """Configure ttk styles with orange-pink theme"""
        style = ttk.Style()
        style.theme_use('clam')
        
        # Configure custom styles
        style.configure('Title.TLabel', 
                       font=('Helvetica', 24, 'bold'),
                       background=self.COLORS['cream'],
                       foreground=self.COLORS['primary_orange'])
        
        style.configure('Subtitle.TLabel',
                       font=('Helvetica', 14, 'bold'),
                       background=self.COLORS['cream'],
                       foreground=self.COLORS['deep_pink'])
        
        style.configure('Regular.TLabel',
                       font=('Helvetica', 11),
                       background=self.COLORS['cream'],
                       foreground=self.COLORS['dark_text'])
        
        style.configure('ActionButton.TButton',
                       font=('Helvetica', 11, 'bold'),
                       padding=10)
        
        style.map('ActionButton.TButton',
                 background=[('active', self.COLORS['deep_pink']),
                            ('pressed', self.COLORS['primary_orange'])])
    
    def create_flower_ui(self):
        """Create the main flower-shaped UI layout"""
        # Main container
        main_frame = tk.Frame(self.root, bg=self.COLORS['cream'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Top Petal - Title
        self.create_title_petal(main_frame)
        
        # Middle Petals - Functionality sections
        middle_frame = tk.Frame(main_frame, bg=self.COLORS['cream'])
        middle_frame.pack(fill=tk.BOTH, expand=True, pady=20)
        
        # Left Petal - Source Selection
        self.create_source_petal(middle_frame)
        
        # Right Petal - Destination Selection
        self.create_destination_petal(middle_frame)
        
        # Bottom Petals - Actions and Progress
        self.create_actions_petals(main_frame)
        
    def create_title_petal(self, parent):
        """Create top petal with title"""
        petal = tk.Frame(parent, bg=self.COLORS['light_orange'], 
                        highlightbackground=self.COLORS['primary_orange'],
                        highlightthickness=3, relief=tk.RAISED)
        petal.pack(fill=tk.X, pady=10)
        
        title_frame = tk.Frame(petal, bg=self.COLORS['light_orange'])
        title_frame.pack(fill=tk.BOTH, padx=20, pady=15)
        
        # Flower emoji and title
        title_label = ttk.Label(title_frame, text="🌸 Digital Migration & Backup 🌸",
                               style='Title.TLabel')
        title_label.pack()
        
        subtitle = ttk.Label(title_frame, 
                           text="Safely migrate and backup your files with beauty",
                           style='Subtitle.TLabel')
        subtitle.pack(pady=5)
        
    def create_source_petal(self, parent):
        """Create left petal for source selection"""
        # Create left and right frame layout
        if not hasattr(parent, '_petals_created'):
            parent._petals_created = True
            parent.pack_configure(fill=tk.BOTH, expand=True)
        
        # Left petal frame
        left_frame = tk.Frame(parent, bg=self.COLORS['cream'])
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10)
        
        petal = tk.Frame(left_frame, bg=self.COLORS['rose_pink'],
                        highlightbackground=self.COLORS['primary_orange'],
                        highlightthickness=2, relief=tk.RAISED)
        petal.pack(fill=tk.BOTH, expand=True)
        
        content = tk.Frame(petal, bg=self.COLORS['rose_pink'])
        content.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        
        # Header
        header = ttk.Label(content, text="📁 Source Location",
                          style='Subtitle.TLabel')
        header.pack(pady=10)
        
        # Path display
        path_frame = tk.Frame(content, bg=self.COLORS['cream'],
                             relief=tk.SUNKEN, borderwidth=2)
        path_frame.pack(fill=tk.X, pady=10)
        
        path_label = tk.Label(path_frame, textvariable=self.source_path,
                             bg=self.COLORS['cream'],
                             fg=self.COLORS['dark_text'],
                             font=('Helvetica', 9),
                             wraplength=180, justify=tk.LEFT)
        path_label.pack(fill=tk.BOTH, padx=8, pady=8)
        
        # Browse button
        browse_source_btn = tk.Button(content, text="Browse Source",
                                     command=self.browse_source,
                                     bg=self.COLORS['primary_orange'],
                                     fg='white',
                                     font=('Helvetica', 10, 'bold'),
                                     relief=tk.RAISED,
                                     padx=10, pady=8,
                                     cursor='hand2')
        browse_source_btn.pack(fill=tk.X, pady=10)
        
        self.source_path.set("Select a folder...")
        
    def create_destination_petal(self, parent):
        """Create right petal for destination selection"""
        # Right petal frame
        right_frame = tk.Frame(parent, bg=self.COLORS['cream'])
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=10)
        
        petal = tk.Frame(right_frame, bg=self.COLORS['light_orange'],
                        highlightbackground=self.COLORS['primary_orange'],
                        highlightthickness=2, relief=tk.RAISED)
        petal.pack(fill=tk.BOTH, expand=True)
        
        content = tk.Frame(petal, bg=self.COLORS['light_orange'])
        content.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        
        # Header
        header = ttk.Label(content, text="💾 Destination Location",
                          style='Subtitle.TLabel')
        header.pack(pady=10)
        
        # Path display
        path_frame = tk.Frame(content, bg=self.COLORS['cream'],
                             relief=tk.SUNKEN, borderwidth=2)
        path_frame.pack(fill=tk.X, pady=10)
        
        path_label = tk.Label(path_frame, textvariable=self.destination_path,
                             bg=self.COLORS['cream'],
                             fg=self.COLORS['dark_text'],
                             font=('Helvetica', 9),
                             wraplength=180, justify=tk.LEFT)
        path_label.pack(fill=tk.BOTH, padx=8, pady=8)
        
        # Browse button
        browse_dest_btn = tk.Button(content, text="Browse Destination",
                                   command=self.browse_destination,
                                   bg=self.COLORS['accent'],
                                   fg='white',
                                   font=('Helvetica', 10, 'bold'),
                                   relief=tk.RAISED,
                                   padx=10, pady=8,
                                   cursor='hand2')
        browse_dest_btn.pack(fill=tk.X, pady=10)
        
        self.destination_path.set("Select a folder...")
        
 def create_actions_petals(self, parent):
    """Create bottom petals for actions and progress"""
    # Actions frame
    actions_frame = tk.Frame(parent, bg=self.COLORS['cream'])
    actions_frame.pack(fill=tk.X, pady=15)

    # Migrate button petal
    migrate_petal = tk.Frame(actions_frame, bg=self.COLORS['deep_pink'],
                             highlightbackground=self.COLORS['primary_orange'],
                             highlightthickness=2)
    migrate_petal.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10)

    migrate_btn = tk.Button(migrate_petal, text="🔄 Migrate Files",
                            command=self.migrate_files,
                            bg=self.COLORS['deep_pink'],
                            fg='white',
                            font=('Helvetica', 12, 'bold'),
                            relief=tk.RAISED,
                            padx=15, pady=15,
                            cursor='hand2',
                            activebackground=self.COLORS['primary_orange'])
    migrate_btn.pack(fill=tk.BOTH, padx=5, pady=5)
    self.migrate_btn = migrate_btn

    # Backup button petal
    backup_petal = tk.Frame(actions_frame, bg=self.COLORS['rose_pink'],
                            highlightbackground=self.COLORS['primary_orange'],
                            highlightthickness=2)
    backup_petal.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=10)

    backup_btn = tk.Button(backup_petal, text="💾 Create Backup",
                           command=self.create_backup,
                           bg=self.COLORS['rose_pink'],
                           fg='white',
                           font=('Helvetica', 12, 'bold'),
                           relief=tk.RAISED,
                           padx=15, pady=15,
                           cursor='hand2',
                           activebackground=self.COLORS['accent'])
    backup_btn.pack(fill=tk.BOTH, padx=5, pady=5)
    self.backup_btn = backup_btn

    # ✅ New checkboxes for backup options
    self.incremental_var = tk.BooleanVar()
    self.compression_var = tk.BooleanVar()

    tk.Checkbutton(backup_petal, text="Incremental Backup",
                   variable=self.incremental_var,
                   bg=self.COLORS['rose_pink'],
                   fg=self.COLORS['dark_text'],
                   font=('Helvetica', 10)).pack(anchor="w", padx=10, pady=2)

    tk.Checkbutton(backup_petal, text="Compress Backup",
                   variable=self.compression_var,
                   bg=self.COLORS['rose_pink'],
                   fg=self.COLORS['dark_text'],
                   font=('Helvetica', 10)).pack(anchor="w", padx=10, pady=2)

        # Progress section
        progress_frame = tk.Frame(parent, bg=self.COLORS['light_orange'],
                                 highlightbackground=self.COLORS['primary_orange'],
                                 highlightthickness=2, relief=tk.SUNKEN)
        progress_frame.pack(fill=tk.X, pady=15)
        
        progress_content = tk.Frame(progress_frame, bg=self.COLORS['light_orange'])
        progress_content.pack(fill=tk.BOTH, padx=15, pady=15)
        
        progress_label = ttk.Label(progress_content, text="📊 Progress",
                                  style='Subtitle.TLabel')
        progress_label.pack(pady=5)
        
        # Progress bar
        progress_style = ttk.Style()
        progress_style.configure("orange.Horizontal.TProgressBar",
                                background=self.COLORS['primary_orange'],
                                troughcolor=self.COLORS['cream'],
                                bordercolor=self.COLORS['primary_orange'],
                                lightcolor=self.COLORS['light_orange'],
                                darkcolor=self.COLORS['primary_orange'])
        
        self.progress_bar = ttk.Progressbar(progress_content, style="orange.Horizontal.TProgressBar",
                                           variable=self.progress_value,
                                           length=400, mode='determinate')
        self.progress_bar.pack(fill=tk.X, pady=10)
        
        # Status label
        self.status_label = tk.Label(progress_content, text="Ready to migrate or backup",
                                    bg=self.COLORS['light_orange'],
                                    fg=self.COLORS['dark_text'],
                                    font=('Helvetica', 10))
        self.status_label.pack(pady=5)
        
    def browse_source(self):
        """Browse and select source folder"""
        folder = filedialog.askdirectory(title="Select Source Folder")
        if folder:
            self.source_path.set(folder)
            self.update_status(f"Source selected: {os.path.basename(folder)}")
    
    def browse_destination(self):
        """Browse and select destination folder"""
        folder = filedialog.askdirectory(title="Select Destination Folder")
        if folder:
            self.destination_path.set(folder)
            self.update_status(f"Destination selected: {os.path.basename(folder)}")
    
    def update_status(self, message):
        """Update status message"""
        self.status_label.config(text=message)
        self.root.update_idletasks()
    
    def migrate_files(self):
        """Migrate files from source to destination"""
        source = self.source_path.get()
        destination = self.destination_path.get()
        
        if not self._validate_paths(source, destination):
            return
        
        if not messagebox.askyesno("Confirm Migration", 
                                   f"Migrate files from:\n{source}\n\nTo:\n{destination}?"):
            return
        
        self.is_running = True
        self.migrate_btn.config(state=tk.DISABLED)
        self.backup_btn.config(state=tk.DISABLED)
        
        thread = threading.Thread(target=self._perform_migration, 
                                 args=(source, destination))
        thread.daemon = True
        thread.start()
    
    def _perform_migration(self, source, destination):
        """Perform file migration in separate thread"""
        try:
            files = list(Path(source).rglob('*'))
            total_files = len([f for f in files if f.is_file()])
            
            if total_files == 0:
                self.update_status("No files to migrate")
                self.is_running = False
                self.migrate_btn.config(state=tk.NORMAL)
                self.backup_btn.config(state=tk.NORMAL)
                return
            
            migrated = 0
            for i, file_path in enumerate(files):
                if file_path.is_file():
                    rel_path = file_path.relative_to(source)
                    dest_path = Path(destination) / rel_path
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(file_path, dest_path)
                    
                    migrated += 1
                    progress = (migrated / total_files) * 100
                    self.progress_value.set(progress)
                    self.update_status(f"Migrating: {rel_path} ({migrated}/{total_files})")
            
            self.update_status(f"✅ Migration completed! {migrated} files migrated.")
            self.progress_value.set(100)
        
        except Exception as e:
            self.update_status(f"❌ Error: {str(e)}")
        
        finally:
            self.is_running = False
            self.migrate_btn.config(state=tk.NORMAL)
            self.backup_btn.config(state=tk.NORMAL)
    
    def create_backup(self):
    source = self.source_path.get()
    destination = self.destination_path.get()

    if not self._validate_paths(source, destination):
        return

    backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    backup_path = os.path.join(destination, backup_name)

    if not messagebox.askyesno("Confirm Backup",
                               f"Create backup at:\n{backup_path}?"):
        return

    self.is_running = True
    self.migrate_btn.config(state=tk.DISABLED)
    self.backup_btn.config(state=tk.DISABLED)

    # ✅ Pass checkbox values into backup thread
    thread = threading.Thread(
        target=self._perform_backup,
        args=(source, backup_path, self.incremental_var.get(), self.compression_var.get())
    )
    thread.daemon = True
    thread.start()

    from backup_engine import BackupEngine
from notification_system import NotificationManager

def _perform_backup(self, source, backup_path, incremental=False, compression=False):
    """Perform backup in separate thread with progress updates"""
    try:
        os.makedirs(backup_path, exist_ok=True)

        notification_manager = NotificationManager()
        engine = BackupEngine(notification_manager)

        # Progress callback for GUI
        def gui_progress(processed, total, rel_path, skipped=False):
            progress = (processed / total) * 100
            self.progress_value.set(progress)
            status = "Skipped" if skipped else "Backing up"
            self.update_status(f"{status}: {rel_path} ({processed}/{total})")

        # Run backup with callback
        success = engine.backup_directory(
            source,
            backup_path,
            incremental=incremental,
            compression=compression,
            progress_callback=gui_progress
        )

        if success and engine.stats:
            stats = engine.stats
            self.update_status(
                f"✅ Backup completed! {stats.total_files} files, "
                f"{BackupEngine._format_size(stats.total_size)}, "
                f"Skipped: {stats.files_skipped}, Errors: {stats.errors}"
            )
            self.progress_value.set(100)
        else:
            self.update_status("❌ Backup failed")

    except Exception as e:
        self.update_status(f"❌ Error: {str(e)}")

    finally:
        self.is_running = False
        self.migrate_btn.config(state=tk.NORMAL)
        self.backup_btn.config(state=tk.NORMAL)
        
    def _validate_paths(self, source, destination):
        """Validate source and destination paths"""
        if source == "Select a folder..." or not os.path.exists(source):
            messagebox.showerror("Invalid Source", "Please select a valid source folder")
            return False
        
        if destination == "Select a folder..." or not os.path.exists(destination):
            messagebox.showerror("Invalid Destination", "Please select a valid destination folder")
            return False
        
        if source == destination:
            messagebox.showerror("Same Path", "Source and destination must be different")
            return False
        
        return True


def main():
    """Main entry point"""
    root = tk.Tk()
    app = FlowerMigrationApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
