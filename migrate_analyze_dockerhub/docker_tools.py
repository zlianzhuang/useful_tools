#!/usr/bin/python3

import os
import json
import requests
import argparse
import logging
import logging.handlers
import time
import subprocess

#################################
# Skopeo can copy image。
#################################
args = None
parse = None
logger = None
amd64 = "amd64"
arm64 = "arm64"
process_download = "download"
process_pull = "pull"
process_push = "push"

def get_repository_file():
    return f"{args.data}/all_repositories"

def init_logger(level: int = logging.DEBUG):
    global logger

    if logger != None:
        return

    logger = logging.getLogger("default")
    logger.setLevel(level)
    formatter = logging.Formatter(
        '%(asctime)s - %(process)d - %(filename)s[func:%(funcName)s, line:%(lineno)d] - %(levelname)s: %(message)s'
    )

    file = "/tmp/docker_tools.log"
    file_handler = logging.handlers.RotatingFileHandler(file,
                                                        maxBytes=20 * 1024 *
                                                        1024,
                                                        backupCount=10)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logstdout = logging.StreamHandler()
    #logstdout.setLevel(logging.WARNING)
    logstdout.setLevel(level)
    logstdout.setFormatter(formatter)
    logger.addHandler(logstdout)


def parse_args():
    global args
    global parser

    parser = argparse.ArgumentParser(description="")
    #group = parser.add_mutually_exclusive_group()
    parser.add_argument("--size",
                       action="store_true",
                       help="get the namespace image size")
    parser.add_argument("--pull",
                       action="store_true",
                       help="pull the namespace image")
    parser.add_argument("--data",
                       help="the data directory")
    parser.add_argument("--download",
                       action="store_true",
                       help="download image")
    parser.add_argument("--namespace",
                       default="radondb",
                        help="docker hub namespace, default: radondb")
    parser.add_argument("--dest-user",
                       help="dest docker hub user")
    parser.add_argument("--dest-password",
                       help="dest docker hub password")
    parser.add_argument("--dest-namespace",
                       default="zlianzhuang",
                        help="docker hub namespace, default: zlianzhuang")
    args = parser.parse_args()


def process_image(process_type = process_download):
    with open(get_repository_file(), "r") as f:
        for line in f:
            if len(line.split(' ')) != 3:
                return

            repository_name = line.split(' ')[0].strip()
            tag_name = line.split(' ')[1].strip()
            multi_arch= line.split(' ')[2].strip()
            logger.info("process %s %s %s" % (repository_name, tag_name, multi_arch))
            #if repository_name == "radondb-postgres-exporter" or repository_name == "busybox":
            if True:
                if multi_arch == "False":
                    image_full_name = f"{args.namespace}/{repository_name}:{tag_name}"
                    image_full_name_dest = f"{args.dest_namespace}/{repository_name}:{tag_name}"
                    image_short_name = f"{repository_name}:{tag_name}"
                    pull_cmd = f"docker pull {image_full_name}"
                    save_cmd = f"docker save {image_full_name} -o {args.data}/{image_short_name}"
                    load_cmd = f"docker load                   -i {args.data}/{image_short_name}"
                    rmi_cmd = f"docker rmi {image_full_name}"
                    rmi_cmd_dest = f"docker rmi {image_full_name_dest}"
                    tag_cmd = f"docker tag {image_full_name} {image_full_name_dest}"
                    push_cmd = f"docker push {image_full_name_dest}"

                    if process_type == process_download:
                        run_command(pull_cmd)
                        run_command(save_cmd)
                        run_command(rmi_cmd)
                    elif process_type == process_pull:
                        run_command(pull_cmd)
                        run_command(rmi_cmd)
                    else:
                        run_command(load_cmd)
                        run_command(tag_cmd)
                        run_command(push_cmd)
                        run_command(rmi_cmd)
                        run_command(rmi_cmd_dest)
                else:
                    image_full_name = f"{args.namespace}/{repository_name}:{tag_name}"
                    image_full_name_dest = f"{args.dest_namespace}/{repository_name}:{tag_name}"
                    image_full_name_dest_transfer_amd64 = f"{args.dest_namespace}/transfer_delete_later:{amd64}"
                    image_full_name_dest_transfer_arm64 = f"{args.dest_namespace}/transfer_delete_later:{arm64}"
                    image_short_name = f"{repository_name}:{tag_name}"

                    pull_amd64 = f"docker pull {image_full_name}"
                    pull_arm64 = f"docker pull --platform {arm64} {image_full_name}"
                    save_amd64 = f"docker save {image_full_name} -o {args.data}/{image_short_name}--{amd64}"
                    load_amd64 = f"docker load                   -i {args.data}/{image_short_name}--{amd64}"
                    save_arm64 = f"docker save {image_full_name} -o {args.data}/{image_short_name}--{arm64}"
                    load_arm64 = f"docker load                   -i {args.data}/{image_short_name}--{arm64}"
                    tag_amd64 = f"docker tag {image_full_name} {image_full_name_dest_transfer_amd64}"
                    tag_arm64 = f"docker tag {image_full_name} {image_full_name_dest_transfer_arm64}"
                    push_amd64 = f"docker push {image_full_name_dest_transfer_amd64}"
                    push_arm64 = f"docker push {image_full_name_dest_transfer_arm64}"
                    rmi_cmd = f"docker rmi {image_full_name}"
                    rmi_amd64 = f"docker rmi {image_full_name_dest_transfer_amd64}"
                    rmi_arm64 = f"docker rmi {image_full_name_dest_transfer_arm64}"
                    create_manifest_cmd = f"docker manifest create {image_full_name_dest} {image_full_name_dest_transfer_amd64} {image_full_name_dest_transfer_arm64}"
                    push_manifest_cmd = f"docker manifest push {image_full_name_dest}"
                    rm_manifest_cmd = f"docker manifest rm {image_full_name_dest}"

                    if process_type == process_download:
                        run_command(pull_amd64)
                        run_command(save_amd64)
                        run_command(rmi_cmd)
                        run_command(pull_arm64)
                        run_command(save_arm64)
                        run_command(rmi_cmd)
                    elif process_type == process_pull:
                        run_command(pull_amd64)
                        run_command(rmi_cmd)
                        run_command(pull_arm64)
                        run_command(rmi_cmd)
                    else:
                        run_command(load_amd64)
                        run_command(tag_amd64)
                        run_command(rmi_cmd)
                        run_command(load_arm64)
                        run_command(tag_arm64)
                        run_command(rmi_cmd)
                        run_command(push_amd64)
                        run_command(push_arm64)
                        run_command(create_manifest_cmd)
                        run_command(push_manifest_cmd)
                        run_command(rm_manifest_cmd)
                        run_command(rmi_amd64)
                        run_command(rmi_arm64)

def run_command(cmd, wait_success = True):
    logger.info(cmd)
    while True:
        ret = ""
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        p.wait()
        for line in p.stdout.readlines():
            ret += line.decode()
        if p.returncode != 0:
            logger.error(cmd + ": run failed.")
            if wait_success == False:
                exit(1)
            else:
                time.sleep(1)
        else:
            break

def gernerate_repository_file():
    total_full_image_size = 0
    total_tag_image_size = 0
    response = requests.get(url=f"https://hub.docker.com/v2/repositories/{args.namespace}?page=1&page_size=1000&ordering=last_updated")
    rj = json.loads(response.text)
    all_repositories = rj["results"]
    if os.path.exists(get_repository_file()) == True:
        logger.error("repositories file exist: %s" % (get_repository_file()))
        exit(1)
    with open(get_repository_file(), "w+") as f:
        for repository in all_repositories:
            repository_name = repository["name"]
            logger.info(f"get {repository_name} tags")
            response_tags = requests.get(url=f"https://hub.docker.com/v2/repositories/{args.namespace}/{repository_name}/tags?page=1&page_size=1000&ordering=last_updated")
            logger.info(f"get {repository_name} tags over")
            rtj = json.loads(response_tags.text)
            all_tags = rtj["results"]
            for tag in all_tags:
                tag_name = tag["name"]
                total_full_image_size += tag["full_size"]
                tag_arch = []
                for arch in tag["images"]:
                    tag_arch.append(arch["architecture"])
                    total_tag_image_size += arch["size"]
                if arm64 in tag_arch and amd64 in tag_arch:
                    multi_arch = True
                else:
                    multi_arch = False
                logger.info("get tag: %s %s %s" % (repository_name, tag_name, multi_arch))
                logger.info("total full size: %d, total tag image size: %d" % (total_full_image_size, total_tag_image_size))
                f.write("%s %s %s\n" % (repository_name, tag_name, multi_arch))
                f.flush()
            time.sleep(1)
    logger.info("%s is generated" % (get_repository_file()))
    logger.info("total full size: %d, total tag image size %d:" % (total_full_image_size, total_tag_image_size))

def source_download():
    gernerate_repository_file()
    process_image(process_type = process_download)

def dry_pull_image():
    gernerate_repository_file()
    process_image(process_type = process_pull)

def push_image():
    run_command(f"docker login -u {args.dest_user} -p {args.dest_password}")
    process_image(process_type = process_push)

def image_size():
    gernerate_repository_file()

def main():
    parse_args()
    init_logger()
    if args.download == True and args.data != None and args.namespace != None:
        source_download()
    elif args.pull == True and args.data != None and args.namespace != None:
        dry_pull_image()
    elif args.dest_user != None and args.dest_password != None and args.data != None and args.namespace != None and args.dest_namespace != None:
        push_image()
    elif args.size == True and args.data != None and args.namespace != None:
        image_size()
    else:
        parser.print_help()

    return 0


if __name__ == '__main__':
    main()

