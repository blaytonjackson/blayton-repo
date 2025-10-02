import os
import paramiko
import stat
from typing import List, Dict, Optional, Union, Tuple


class SFTPManager:
    """
    A class to manage SFTP files and directories.
    
    This class provides methods to connect to an SFTP server and perform 
    various operations such as uploading, downloading, listing, and 
    deleting files and directories.
    """
    
    def __init__(self):
        """Initialize the SFTPManager with empty connection attributes."""
        self.client = None
        self.sftp = None
        self.connected = False
    
    def connect(self, hostname: str, port: int = 22, username: str = None, 
                password: str = None, key_filename: str = None, 
                passphrase: str = None, timeout: int = 30) -> bool:
        """
        Connect to an SFTP server.
        
        Args:
            hostname: The hostname or IP address of the SFTP server.
            port: The port number of the SFTP server (default: 22).
            username: The username for authentication.
            password: The password for authentication (if using password auth).
            key_filename: Path to a private key file (if using key-based auth).
            passphrase: Passphrase for the private key (if needed).
            timeout: Connection timeout in seconds.
            
        Returns:
            bool: True if connection is successful, False otherwise.
        """
        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            connect_kwargs = {
                'hostname': hostname,
                'port': port,
                'timeout': timeout
            }
            
            if username:
                connect_kwargs['username'] = username
            if password:
                connect_kwargs['password'] = password
            if key_filename:
                connect_kwargs['key_filename'] = key_filename
            if passphrase:
                connect_kwargs['passphrase'] = passphrase
                
            self.client.connect(**connect_kwargs)
            self.sftp = self.client.open_sftp()
            self.connected = True
            return True
            
        except Exception as e:
            print(f"Connection failed: {str(e)}")
            self.connected = False
            return False
    
    def disconnect(self) -> None:
        """Close the SFTP and SSH connections."""
        if self.sftp:
            self.sftp.close()
        
        if self.client:
            self.client.close()
            
        self.connected = False
    
    def _check_connection(self) -> None:
        """Check if the connection is established."""
        if not self.connected or not self.sftp:
            raise ConnectionError("Not connected to SFTP server. Call connect() first.")
    
    def list_directory(self, remote_path: str = '.') -> List[Dict]:
        """
        List files and directories in the specified remote path.
        
        Args:
            remote_path: The remote directory path to list (default: current directory).
            
        Returns:
            A list of dictionaries with file/directory information.
        """
        self._check_connection()
        
        items = []
        for item in self.sftp.listdir_attr(remote_path):
            mode = item.st_mode
            item_type = 'directory' if stat.S_ISDIR(mode) else 'file'
            
            items.append({
                'name': item.filename,
                'type': item_type,
                'size': item.st_size,
                'permissions': stat.filemode(mode),
                'modified': item.st_mtime
            })
            
        return items
    
    def mkdir(self, remote_path: str, mode: int = 0o777) -> bool:
        """
        Create a directory on the remote server.
        
        Args:
            remote_path: The remote directory path to create.
            mode: The permissions to set for the directory.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        self._check_connection()
        
        try:
            self.sftp.mkdir(remote_path, mode)
            return True
        except Exception as e:
            print(f"Failed to create directory: {str(e)}")
            return False
    
    def mkdir_p(self, remote_path: str, mode: int = 0o777) -> bool:
        """
        Create a directory and any parent directories that don't exist.
        
        Args:
            remote_path: The remote directory path to create.
            mode: The permissions to set for the directory.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        self._check_connection()
        
        if remote_path == '/' or remote_path == '':
            return True
            
        try:
            self.sftp.stat(remote_path)
            return True  # Directory already exists
        except IOError:
            # Directory doesn't exist, create parent directory first
            parent = os.path.dirname(remote_path)
            if parent and parent != remote_path:
                self.mkdir_p(parent, mode)
                
            return self.mkdir(remote_path, mode)
    
    def upload_file(self, local_path: str, remote_path: str, 
                   preserve_mtime: bool = False) -> bool:
        """
        Upload a file to the remote server.
        
        Args:
            local_path: The local file path.
            remote_path: The remote file path.
            preserve_mtime: Whether to preserve the file's modification time.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        self._check_connection()
        
        try:
            # Create parent directory if it doesn't exist
            remote_dir = os.path.dirname(remote_path)
            if remote_dir:
                self.mkdir_p(remote_dir)
                
            # Upload the file
            self.sftp.put(local_path, remote_path)
            
            # Preserve modification time if requested
            if preserve_mtime:
                local_stat = os.stat(local_path)
                self.sftp.utime(remote_path, (local_stat.st_atime, local_stat.st_mtime))
                
            return True
        except Exception as e:
            print(f"Failed to upload file: {str(e)}")
            return False
    
    def download_file(self, remote_path: str, local_path: str, 
                     preserve_mtime: bool = False) -> bool:
        """
        Download a file from the remote server.
        
        Args:
            remote_path: The remote file path.
            local_path: The local file path.
            preserve_mtime: Whether to preserve the file's modification time.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        self._check_connection()
        
        try:
            # Create local directory if it doesn't exist
            local_dir = os.path.dirname(local_path)
            if local_dir and not os.path.exists(local_dir):
                os.makedirs(local_dir)
                
            # Download the file
            self.sftp.get(remote_path, local_path)
            
            # Preserve modification time if requested
            if preserve_mtime:
                remote_stat = self.sftp.stat(remote_path)
                os.utime(local_path, (remote_stat.st_atime, remote_stat.st_mtime))
                
            return True
        except Exception as e:
            print(f"Failed to download file: {str(e)}")
            return False
    
    def delete_file(self, remote_path: str) -> bool:
        """
        Delete a file on the remote server.
        
        Args:
            remote_path: The remote file path to delete.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        self._check_connection()
        
        try:
            self.sftp.remove(remote_path)
            return True
        except Exception as e:
            print(f"Failed to delete file: {str(e)}")
            return False
    
    def delete_directory(self, remote_path: str, recursive: bool = False) -> bool:
        """
        Delete a directory on the remote server.
        
        Args:
            remote_path: The remote directory path to delete.
            recursive: Whether to recursively delete the directory contents.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        self._check_connection()
        
        try:
            if not recursive:
                self.sftp.rmdir(remote_path)
                return True
                
            # Recursive delete
            items = self.list_directory(remote_path)
            for item in items:
                item_path = os.path.join(remote_path, item['name'])
                if item['type'] == 'directory':
                    self.delete_directory(item_path, recursive=True)
                else:
                    self.delete_file(item_path)
                    
            self.sftp.rmdir(remote_path)
            return True
        except Exception as e:
            print(f"Failed to delete directory: {str(e)}")
            return False
    
    def rename(self, remote_src: str, remote_dst: str) -> bool:
        """
        Rename/move a file or directory on the remote server.
        
        Args:
            remote_src: The source path.
            remote_dst: The destination path.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        self._check_connection()
        
        try:
            self.sftp.rename(remote_src, remote_dst)
            return True
        except Exception as e:
            print(f"Failed to rename/move: {str(e)}")
            return False
    
    def file_exists(self, remote_path: str) -> bool:
        """
        Check if a file exists on the remote server.
        
        Args:
            remote_path: The remote file path to check.
            
        Returns:
            bool: True if the file exists, False otherwise.
        """
        self._check_connection()
        
        try:
            self.sftp.stat(remote_path)
            return True
        except IOError:
            return False
    
    def get_file_info(self, remote_path: str) -> Optional[Dict]:
        """
        Get information about a file or directory on the remote server.
        
        Args:
            remote_path: The remote path to check.
            
        Returns:
            A dictionary with file/directory information or None if not found.
        """
        self._check_connection()
        
        try:
            stat_result = self.sftp.stat(remote_path)
            mode = stat_result.st_mode
            item_type = 'directory' if stat.S_ISDIR(mode) else 'file'
            
            return {
                'name': os.path.basename(remote_path),
                'type': item_type,
                'size': stat_result.st_size,
                'permissions': stat.filemode(mode),
                'modified': stat_result.st_mtime
            }
        except IOError:
            return None
    
    def upload_directory(self, local_path: str, remote_path: str, 
                        preserve_mtime: bool = False) -> bool:
        """
        Upload a directory and its contents to the remote server.
        
        Args:
            local_path: The local directory path.
            remote_path: The remote directory path.
            preserve_mtime: Whether to preserve file modification times.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        if not os.path.isdir(local_path):
            print(f"Local path is not a directory: {local_path}")
            return False
            
        # Create remote directory if it doesn't exist
        self.mkdir_p(remote_path)
        
        success = True
        for root, dirs, files in os.walk(local_path):
            # Calculate the relative path from local_path
            rel_path = os.path.relpath(root, local_path)
            if rel_path == '.':
                rel_path = ''
                
            # Create directories
            for dir_name in dirs:
                remote_dir = os.path.join(remote_path, rel_path, dir_name)
                if not self.mkdir_p(remote_dir):
                    success = False
                    
            # Upload files
            for file_name in files:
                local_file = os.path.join(root, file_name)
                remote_file = os.path.join(remote_path, rel_path, file_name)
                if not self.upload_file(local_file, remote_file, preserve_mtime):
                    success = False
                    
        return success
    
    def download_directory(self, remote_path: str, local_path: str, 
                          preserve_mtime: bool = False) -> bool:
        """
        Download a directory and its contents from the remote server.
        
        Args:
            remote_path: The remote directory path.
            local_path: The local directory path.
            preserve_mtime: Whether to preserve file modification times.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        # Create local directory if it doesn't exist
        if not os.path.exists(local_path):
            os.makedirs(local_path)
            
        success = True
        items = self.list_directory(remote_path)
        
        for item in items:
            remote_item_path = os.path.join(remote_path, item['name'])
            local_item_path = os.path.join(local_path, item['name'])
            
            if item['type'] == 'directory':
                if not self.download_directory(remote_item_path, local_item_path, preserve_mtime):
                    success = False
            else:
                if not self.download_file(remote_item_path, local_item_path, preserve_mtime):
                    success = False
                    
        return success
    
    def chmod(self, remote_path: str, mode: int) -> bool:
        """
        Change the permissions of a file or directory on the remote server.
        
        Args:
            remote_path: The remote path.
            mode: The file permissions (e.g., 0o644, 0o755).
            
        Returns:
            bool: True if successful, False otherwise.
        """
        self._check_connection()
        
        try:
            self.sftp.chmod(remote_path, mode)
            return True
        except Exception as e:
            print(f"Failed to change permissions: {str(e)}")
            return False