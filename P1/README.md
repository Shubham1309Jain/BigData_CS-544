# P1 Solution Notes: Counting Loans with Dockerized Shell Script

## Part 0: VM Setup and SSH Connection (GCP)

In this section, I document the process of setting up a Google Cloud Platform (GCP) Virtual Machine (VM), reserving a static IP (Elastic IP), and connecting to the instance using SSH via key-based authentication.

> **Note**: The CS544 course provides a preconfigured DoIT VM environment for enrolled students.  
> Since I am not officially enrolled in the course, I created my own virtual environment using GCP instead.


### Steps:

#### 1. Create the VM on GCP
- Follow the configuration steps from the previous semester's project documentation:  
  [cs544-wisc/s24 Project 1](https://github.com/cs544-wisc/s24/tree/main/p1)

#### 2. Reserve a Static (Elastic) IP
- Navigate to **VPC Network** > **IP addresses** > **Reserve a static address**.
- Name the IP and attched to the VM you just created.

#### 3. Generate SSH key locally
```bash
ssh-keygen -C "your_email@example.com"
# -C: Adds a comment (usually your email) to help identify the key.
```

Output: two files in the `~/.ssh` directory:

- **Private key**: `~/.ssh/id_ed25519` → used on the machine, do **not** upload.
- **Public key**: `~/.ssh/id_ed25519.pub` → this is what you paste into GitHub.

For setting up an SSH connection shortcut, refer to: [How To Create an SSH Shortcut](https://www.digitalocean.com/community/tutorials/how-to-create-an-ssh-shortcut)

#### 4. Set up public key in VM
- Copy the contents of the **public key** (e.g., `id_ed25519.pub`) into the VM’s SSH metadata field.
- Make sure the **username** in the key comment matches what you use in SSH commands.

#### 5. Set up firewall configuration (just like AWS security group)
- VPC Network > Firewall 
- Then change the network tag of VM

#### 6. Set up github connection in VM
- Set up your customized user name and email
  ```bash
  git config --global user.name "Little Cat"
  git config --global user.email "lcat@gmail.com"
  ```
- Generate SSH Key on the VM (it's VM, not local!)
- Add the Public Key to GitHub
  - Settings > SSH and GPG keys > Add your public key information
- Set up your SSH config in server
  ```bash
  vim ~/.ssh/config
  ```
  Add the following:
  ```bash
  Host github.com
  HostName github.com
  User <yourusername>
  IdentityFile ~/.ssh/id_cs544_vm # the path you keep your private key
  ```

- Use this command to verify your SSH key to GitHub:
  ```bash
  ssh -T git@github.com
  ````

### ✅ Extra Notes & Pitfalls

#### Disk Usage
```bash
df -h  # Show disk usage in human-readable format
```

#### Executing Scripts
```
./download.sh  # The `./` means current directory
```

#### Git: Large File Error Recovery
If Git rejects your push due to file size (>100MB), follow this sequence:

```
# Create an orphan branch (no commit history)
git checkout --orphan clean-main

# Add files selectively, skipping large ones
git add . --all
git reset code/wi*.csv
git reset code/wi.txt

# Commit clean state
git commit -m "Clean version without large files"

# Force push clean branch to main (⚠️ Better: checkout main & merge)
git push -f origin clean-main:main
```


> **Reflection**: In the past, I’ve worked with AWS cloud services, so I’m already familiar with the general process of provisioning virtual machines, setting up SSH access, and working in a cloud-based development environment. I found interesting is that on GCP, the process of adding a public key to the instance metadata was slightly different compared to AWS’s use of pre-defined .pem key files. With GCP, I generated the SSH key pair manually using ssh-keygen and pasted the public key into the VM settings.