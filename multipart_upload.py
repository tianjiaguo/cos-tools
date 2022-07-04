# -*- coding: utf-8 -*-

# --file_dir=/tmp --file_name=go1.16.3.linux-amd64.tar.gz --cos_key=/test/go/1.16.3/app.tar.gz --upload_id=1618917734811cee50bf969ec5480f42a9c099c27368fc7d37a7258e3907dc095d3d8298b7

import logging
import os
import sys
import shelve

from optparse import OptionParser
from qcloud_cos import CosS3Client, CosConfig, CosServiceError

# 初始化日志
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


user_params = {}


def get_user_params():
    try:
        opt = OptionParser()
        opt.add_option("--file_dir", dest='file_dir', type=str, help='待上传文件目录')
        opt.add_option("--file_name", dest='file_name', type=str, help='待上传文件名')
        opt.add_option("--cos_key", dest='cos_key', type=str, help='最终在cos上的key')
        opt.add_option("--upload_id", dest='upload_id', type=str, help='如果上次上传失败，可使用此ID断点续传')
        (options, args) = opt.parse_args()
        is_valid_paras = True
        error_message = []
        file_dir = options.file_dir
        file_name = options.file_name
        cos_key = options.cos_key
        upload_id = options.upload_id
        if not file_dir:
            logger.error("待上传文件目录未设置，格式：/xxx/yyy")
            is_valid_paras = False
        if not file_name:
            logger.error("待上传文件名未设置，格式：xxxx.xx")
            is_valid_paras = False
        if not cos_key:
            logger.error("最终在cos上的key未设置，格式：/xxxx/xxx/xxxx.xx")
            is_valid_paras = False
        if upload_id:
            logger.info("使用断点续传[" + upload_id + "]")
        if is_valid_paras:
            user_params = {
                "file_dir": file_dir,
                "file_name": file_name,
                "cos_key": cos_key,
                "upload_id": upload_id,
            }
            return user_params
        else:
            opt.print_help()
            return None
    except Exception as ex:
        logger.error("解析参数失败:{0}".format(str(ex)))
        return None

def initProcessParam(UploadId):
    parts = []
    if os.path.exists("/tmp/" + UploadId + ".db"):
        process_file = shelve.open("/tmp/" + UploadId)
        if process_file["parts"]:
            parts = process_file["parts"]
        process_file.close()
    return parts, len(parts) + 1

def saveUploadProcess(UploadId, Parts):
    process_file = shelve.open("/tmp/" + UploadId)
    process_file["parts"] = Parts
    process_file.close()
    return parts

def getUploadProcess(UploadId):
    process_file = shelve.open("/tmp/" + UploadId)
    parts = process_file["parts"]
    process_file.close()
    return parts

def commitUploadProcess(UploadId):
    file_name = "/tmp/" + UploadId + ".db"
    if os.path.exists(file_name):
        os.remove(file_name)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    logger.info("开始执行脚本")
    user_params = get_user_params()
    if not user_params:
        sys.exit(1)

    region = "ap-beijing"
    secretId = "xxxxxx"
    secretKey = "yyyyyyy"

    bucket = "ipc-xxxx-11111111"
    # 创建client
    config = CosConfig(Region=region, SecretId=secretId, SecretKey=secretKey, Token=None)
    client = CosS3Client(config)
    # 上传文件位置和目标key
    file_dir = user_params["file_dir"]
    file_name = user_params["file_name"]
    cos_key = user_params["cos_key"]
    # file_dir = "/tmp"
    # file_name = "goland-2020.3.4.dmg"
    # cos_key = "/test/d/goland/2020.3.4/app.dmg"
    # 初始化
    upload_id = user_params["upload_id"]
    if not upload_id:
        response = client.create_multipart_upload(Bucket=bucket, Key=cos_key)
        upload_id = response["UploadId"]
    logger.info("文件上传ID:=>" + upload_id)
    # 分块上传，拿上传进度
    parts,next_part_num = initProcessParam(UploadId=upload_id)
    pnum = 1
    #
    upload_file = file_dir + "/" + file_name
    file_size = os.path.getsize(upload_file)
    #
    # 开始分块上传
    with open(upload_file, 'rb') as fp:
        while 1:
            data = fp.read(20 * 1024 * 1024)
            if not data:
                break
            if pnum < next_part_num:
                pnum = pnum + 1
                continue
            logger.info("开始上传[" + upload_id + "]的第[" + str(pnum) + "]个分片")
            response = client.upload_part(Bucket=bucket, Body=data, Key=cos_key, PartNumber=pnum, UploadId=upload_id)
            etag = response['ETag']
            parts.append({'ETag': etag, 'PartNumber': pnum})
            # 保存进度
            saveUploadProcess(UploadId=upload_id, Parts=parts)
            logger.info("结束上传[" + upload_id + "]的第[" + str(pnum) + "]个分片")
            pnum = pnum + 1
    # 读取进度并提交
    try:
        response = client.complete_multipart_upload(Bucket=bucket, Key=cos_key, UploadId=upload_id, MultipartUpload={
            'Part': getUploadProcess(UploadId=upload_id)
        })
    except CosServiceError as ex:
        logger.error("断点续传不存在")
    commitUploadProcess(UploadId=upload_id)
    print_hi('finish[' + upload_id + ']')


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
