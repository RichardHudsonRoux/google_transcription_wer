import sys
from google.cloud import storage, bigquery
import pandas as pd
import json
import simple_wer_v2 as wer

def GCS():
    return storage.Client.from_service_account_json('./gcloud/retail-call-transcription-poc-key.json')

def GBQ():
    return bigquery.Client.from_service_account_json('./gcloud/retail-call-transcription-poc-key.json')

def download_into_memory(bucket_name, blob_name):
    storage_client = GCS()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    contents = blob.download_as_bytes()
    return contents

    lineAnalysis = wer.SimpleWER(
        key_phrases=None,
        html_handler=wer.HighlightAlignedHtmlHandler(wer.HighlightAlignedHtml),
        preprocess_handler = wer.RemoveCommentTxtPreprocess)
    lineAnalysis.AddHypRef(hyp, ref)
    response = lineAnalysis.GetBreakdownWER()
    if(response["wer"]>0):
        response["cor"] = lineAnalysis.aligned_htmls[0]
    else:
        response["cor"] = ""
    return response

def insert_into_bigquery(data):
    bigquery_client = GBQ();
    errors = bigquery_client.insert_rows_json("retail-call-transcription-poc.CallData.Analysis", data)
    if errors == []:
        print("New rows added")
    else: 
        print("Encountered error: {}".format(errors))

def getWERbyLine(hyp, ref):
    lineAnalysis = wer.SimpleWER(
        key_phrases=None,
        html_handler=wer.HighlightAlignedHtmlHandler(wer.HighlightAlignedHtml),
        preprocess_handler = wer.RemoveCommentTxtPreprocess)
    lineAnalysis.AddHypRef(hyp, ref)
    response = lineAnalysis.GetBreakdownWER()
    if(response["wer"]>0):
        response["cor"] = lineAnalysis.aligned_htmls[0]
    else:
        response["cor"] = ""
    return response


def runWER(event, context):
    file = event

    jobnumber = file['name'].split('/')[0]
    callid = file['name'].split(".")[0]

    wholeAnalysis = wer.SimpleWER(
        key_phrases=None,
        html_handler=wer.HighlightAlignedHtmlHandler(wer.HighlightAlignedHtml),
        preprocess_handler = wer.RemoveCommentTxtPreprocess)

    #get file from event
    df = pd.read_excel(download_into_memory("transcripts-truths", "rjh_test/CAcf16bb76f30539211eff1d766e831f09.wav.transcript.hypothesis.xlsx"))

    #run Analysis
    
    utteranceData=[]
    html = ""
    for index, row  in df.iterrows():
        werDict = getWERbyLine(row[2], row[3])
        wholeAnalysis.AddHypRef(row[2], row[3])
        thisData = {
            "callSid": callid,
            "jobNumber": jobnumber,
            "channel": "", 
            "timeOffset": row[1], 
            "hypothesis": row[2], 
            "reference": row[3], 
            "insertions": werDict['ins'], 
            "deletions": werDict['del'], 
            "substitutions": werDict['sub'], 
            "wordCount": werDict['wct'], 
            "errorCount": werDict['erc'], 
            "wordErrorRate": werDict['wer'],
            "corrections": werDict['cor']
        }
        if row[0]==2:
            thisData["channel"] = "Agent"
            html += """<div class="message speaker2"><span class="speaker">Agent</span>"""
        else:
            thisData["channel"] = "Customer"
            html += """<div class="message speaker1"><span class="speaker">Customer</span>"""
        
        if "".__eq__(thisData["corrections"]):
            html += """<span class="message-text">%s</span>""" % (thisData["hypothesis"])
            html += """<span class="reference_title">Reference:</span>"""
            html += """<span class="reference">%s</span>""" % (thisData["reference"])
        else:
            html += """<span class="message-text">%s</span>""" % (thisData["corrections"])
            html += """<span class="reference_title">Hypothesis:</span>"""
            html += """<span class="reference">%s</span>""" % (thisData["hypothesis"])
            html += """<span class="reference_title">Reference:</span>"""
            html += """<span class="reference">%s</span>""" % (thisData["reference"])

        html += """<span class="timing">WER = %.2f%%, word count = %d, error count = %d, insertions = %d, deletions = %d, substitutions = %d</span></div>""" % (
            werDict['wer'], werDict['wct'], werDict['erc'], werDict['ins'], werDict['del'], werDict['sub'])
        
        utteranceData.append(thisData)

    conversationAnalysis = wholeAnalysis.GetBreakdownWER()
    docTotals = 'Summary:  WER = %.2f%%, word count = %d, error count = %d, insertions = %d, deletions = %d, substitutions = %d' % (
            conversationAnalysis['wer'], conversationAnalysis['wct'], conversationAnalysis['erc'], conversationAnalysis['ins'], conversationAnalysis['del'], conversationAnalysis['sub'])

    html = """<!DOCTYPE html><html><head>
        <title>Conversation """ + callid + """</title>
        <style>.container{max-width:800px;margin:0 auto;padding:20px;font-family:Arial,sans-serif}.transcript{display:flex;flex-direction:column}.message{display:flex;flex-direction:column;margin-bottom:10px}.speaker{font-weight:700;text-align:center}.message-text{margin-top:5px;margin-bottom:5px}.timing{color:#999;font-size:.6em;text-align:center}.speaker1{align-items:flex-end}.speaker2{align-items:flex-start}.reference {font-size: 0.7em; font-style: italic;}.reference_title {font-size: 0.7em;}</style></head><body>
        <div class="container">
        <h2>Conversation """ + callid + """</h2>
        <div class="timing">""" + docTotals + """</div>
        <div class="transcript">""" + html + """</div></div></body></html>""" 

    insert_into_bigquery(utteranceData)

    #record analysis
    #print(json.dumps(utteranceData))
    #print(conversationAnalysis)
    #print(html)





event = {'bucket': 'transcripts-truths', 'contentType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'crc32c': 'Y0kfxw==', 'etag': 'CO3P87/Qvf8CEAE=', 'generation': '1686569366644717', 'id': 'transcripts-truths/rjh_test/CAcf16bb76f30539211eff1d766e831f09.wav.transcript.hypothesis.xlsx/1686569366644717', 'kind': 'storage#object', 'md5Hash': 'Mb7vADnJJAFXk5RXdXdUVA==', 'mediaLink': 'https://storage.googleapis.com/download/storage/v1/b/transcripts-truths/o/rjh_test%2FCAcf16bb76f30539211eff1d766e831f09.wav.transcript.hypothesis.xlsx?generation=1686569366644717&alt=media', 'metageneration': '1', 'name': 'rjh_test/CAcf16bb76f30539211eff1d766e831f09.wav.transcript.hypothesis.xlsx', 'selfLink': 'https://www.googleapis.com/storage/v1/b/transcripts-truths/o/rjh_test%2FCAcf16bb76f30539211eff1d766e831f09.wav.transcript.hypothesis.xlsx', 'size': '23253', 'storageClass': 'STANDARD', 'timeCreated': '2023-06-12T11:29:26.653Z', 'timeStorageClassUpdated': '2023-06-12T11:29:26.653Z', 'updated': '2023-06-12T11:29:26.653Z'}
runWER(event, {})








    







#write to BigQuery

#output files to storage