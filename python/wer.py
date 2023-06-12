import simple_wer_v2 as wer
import pandas as pd
import json

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


wholeAnalysis = wer.SimpleWER(
    key_phrases=None,
    html_handler=wer.HighlightAlignedHtmlHandler(wer.HighlightAlignedHtml),
    preprocess_handler = wer.RemoveCommentTxtPreprocess)

#get hyposthesisfiles
##load byte array from local excel file
file=open("./transcripts/CAcf16bb76f30539211eff1d766e831f09.wav.transcript.hypothesis.xlsx", "rb")
xlBytes=file.read() 
file.close()
callid = "CAcf16bb76f30539211eff1d766e831f09"


#Add Hpothesis and References to Analysis Engine
utteranceData=[]
html = ""
df = pd.read_excel(xlBytes)
for index, row  in df.iterrows():
    werDict = getWERbyLine(row[2], row[3])
    wholeAnalysis.AddHypRef(row[2], row[3])
    thisData = {
        "Channel": "", 
        "TimeOffset": row[1], 
        "Hypothesis": row[2], 
        "Reference": row[3], 
        "Insertions": werDict['ins'], 
        "Deletions": werDict['del'], 
        "Substitutions": werDict['sub'], 
        "Wordcount": werDict['wct'], 
        "Errorcount": werDict['erc'], 
        "WordErrorRate": werDict['wer'],
        "Corrections": werDict['cor']
    }
    if row[0]==2:
        thisData["Channel"] = "Agent"
        html += """<div class="message speaker2"><span class="speaker">Agent</span>"""
    else:
        thisData["Channel"] = "Customer"
        html += """<div class="message speaker1"><span class="speaker">Customer</span>"""
    
    if "".__eq__(thisData["Corrections"]):
        html += """<span class="message-text">%s</span>""" % (thisData["Hypothesis"])
        html += """<span class="reference_title">Reference:</span>"""
        html += """<span class="reference">%s</span>""" % (thisData["Reference"])
    else:
        html += """<span class="message-text">%s</span>""" % (thisData["Corrections"])
        html += """<span class="reference_title">Hypothesis:</span>"""
        html += """<span class="reference">%s</span>""" % (thisData["Hypothesis"])
        html += """<span class="reference_title">Reference:</span>"""
        html += """<span class="reference">%s</span>""" % (thisData["Reference"])

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




#print(html)
#print(conversationAnalysis)
#print(json.dumps(utteranceData))





