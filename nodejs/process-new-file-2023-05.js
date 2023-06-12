
const { S3Client, GetObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { Storage } = require('@google-cloud/storage');
const Speech = require('@google-cloud/speech');
const Insights = require('@google-cloud/contact-center-insights').v1;
const { BigQuery } = require('@google-cloud/bigquery');
const { once } = require('events'); 

let clientGCS = () => {return new Storage(); };
let clientS3  = () => {return new S3Client({region: 'eu-west-2',credentials: {accessKeyId: process.env.AWS_S3_ACCESS_KEY_ID,secretAccessKey: process.env.AWS_S3_SECRET_ACCESS_KEY}}); };
let clientSPEECH = (transcriptConfig) => { 
    return transcriptConfig.VERSION=="V2" ?
        new Speech.v2.SpeechClient({projectId:  process.env.PROJECT_ID, apiEndpoint: transcriptConfig.API_ENDPOINT}) :
        new Speech.v1.SpeechClient({projectId: process.env.PROJECT_ID}); 
    };
let clientINSIGHTS = () => { return new Insights.ContactCenterInsightsClient({apiEndpoint: "europe-west2-contactcenterinsights.googleapis.com"}); };
let clientBIGQUERY = () => { return new BigQuery({apiEndpoint: "europe-west2-bigquery.googleapis.com"}); };


//Run Transcript Job
let _getRecordingFileReference = async (gClient, aClient, transcriptionConfig, jobId, destPath) => {
    console.debug(`Looking for ${destPath} in gs://${process.env.SECURE_BUCKET}`)
    const isInGcs = await gClient.bucket(process.env.SECURE_BUCKET).file(destPath).getMetadata().then(()=>{return true;}).catch(()=>{return false;});
    let awsSourcePath = '';
    if(!isInGcs){
        console.debug(`Not found in GCS`)
        console.debug(`Looking for ${destPath} in s3://${process.env.S3_BUCKET}`)
        const isInAwsRoot = await aClient.send(new HeadObjectCommand({Bucket: process.env.S3_BUCKET, Key: destPath})).then(()=>{return true;}).catch(()=>{return false;});
        if(!isInAwsRoot){
            console.debug(`Not found in S3 Root Folder`)
            console.debug(`Looking for ${destPath} in gs://${process.env.S3_BUCKET}`)
            const isInAwsDigitalVoice = await aClient.send(new HeadObjectCommand({Bucket: process.env.S3_BUCKET, Key: `calls-to-digitalvoice/${destPath}`})).then(()=>{return true;}).catch(()=>{return false;});
            if(!isInAwsDigitalVoice){
                console.debug(`Not found in S3 Digital Voice Folder`);
                console.debug(`No recording found for ${destPath.split('.')[0]}`);
                return {bucket: null, name: null};
            } else {
                console.debug(`Found in S3 Digital Voice Folder`);
                awsSourcePath = `calls-to-digitalvoice/${destPath}`;
            }
        } else {
            console.debug(`Found in S3 Root Folder`);
            awsSourcePath = destPath
        }
    } else {
        console.debug(`Found in GCS`);
    }
    if(awsSourcePath != ''){
        try {
            const writeStream = gClient.bucket(process.env.SECURE_BUCKET).file(destPath).createWriteStream({resumable: false,validation: false,metadata: {contentType: 'audio/wav'}});
            const response = await aClient.send(new GetObjectCommand({
                Bucket: process.env.S3_BUCKET,
                Key: awsSourcePath
            }));
            const result = response.Body.pipe(writeStream);
            await once(result, 'finish');
            console.debug(`Transfered s3://${process.env.S3_BUCKET}/${awsSourcePath} to gs://${process.env.SECURE_BUCKET}/${destPath}`);
        } catch (error) {
            console.warn("ERROR");
            console.debug(error);
            return {};
        }
    }
    let rtn = {
        bucket: process.env.SECURE_BUCKET, 
        name: destPath,
        audioPath: `gs://${process.env.SECURE_BUCKET}/${destPath}`,
        transcriptPath: (transcriptionConfig.VERSION=="V2" ? `gs://${process.env.SECURE_BUCKET}/${jobId}` : `gs://${process.env.SECURE_BUCKET}/${jobId}/${destPath}.transcript.json`)
    };
    return rtn;
}

let _transcribeRecording = async (speechClient, transcriptionConfig, fileRef) => {
    if(transcriptionConfig.VERSION=="V2"){
        console.debug(`Requesting transcript for ${fileRef.name} using V2 - ${transcriptionConfig.V2_RECOGNISER}`);
        let speechResult = await speechClient.batchRecognize({
            recognizer: `projects/${process.env.PROJECT_ID}/locations/${transcriptionConfig.LOCATION}/recognizers/${transcriptionConfig.V2_RECOGNISER}`,
            config: transcriptionConfig.V2_CONFIG,
            files: [{"uri":`gs://${process.env.SECURE_BUCKET}/${fileRef.name}`}],
            recognitionOutputConfig: {gcsOutputConfig: {uri: `gs://${process.env.SECURE_BUCKET}/${fileRef.jobid}`}}
        });
        return {
            audioPath: `gs://${process.env.SECURE_BUCKET}/${fileRef.name}`, 
            transcriptPath: `gs://${process.env.SECURE_BUCKET}/${fileRef.jobid}`
        }
    }
    if(transcriptionConfig.VERSION=="V1"){
        console.debug(`Requesting transcript for ${fileRef.name} using V1 - ${JSON.stringify(transcriptionConfig.V1_CONFIG)}`);
        let speechResult = await speechClient.longRunningRecognize({
            audio: {uri: `gs://${process.env.SECURE_BUCKET}/${fileRef.name}`},
            config: transcriptionConfig.V1_CONFIG,
            outputConfig: {gcsUri:`gs://${process.env.SECURE_BUCKET}/${fileRef.jobid}/${fileRef.name}.transcript.json`}
        })
        return {
            ...fileRef,
            audioPath: `gs://${process.env.SECURE_BUCKET}/${fileRef.name}`, 
            transcriptPath: `gs://${process.env.SECURE_BUCKET}/${fileRef.jobid}/${fileRef.name}.transcript.json`
        }
    }
    return {};
}

let runTransciptProcess = async (gcsEvent) => {
    let gClient = clientGCS();
    let aClient = clientS3();

    console.debug(`Runing new Transcription Job ${JSON.stringify(gcsEvent)}`);
    console.debug('- getting configuration');
    let config = JSON.parse(await gClient.bucket(gcsEvent.bucket).file(gcsEvent.name).download());
    console.debug(`- configuration found: ${JSON.stringify(config)}`);
    
    let callPromises = [];
    let fileReferences = [];
    let transcriptPromises = [];
    let transcriptErrors = [];
    let rtn = {};

    config.calls.forEach((call) => {
        callPromises.push(new Promise(async (res, rej)=>{
            let thisRef = await _getRecordingFileReference(gClient, aClient, config.TRANSCRIPT_CONFIG, gcsEvent.id, `${call.callId}.wav`);
            thisRef = {
                jobid: gcsEvent.id,
                callid: call.callId,
                ...thisRef,
                call: call
            }
            console.debug(`Found ${JSON.stringify(thisRef.name)}`);
            fileReferences.push(thisRef);
            res();
        }));
    });

    await Promise.all(callPromises).then( async ()=>{
        await gClient.bucket(process.env.SECURE_BUCKET).file(`${gcsEvent.id}/${gcsEvent.id}.results.json`).save(JSON.stringify(fileReferences));
        await gClient.bucket(process.env.SECURE_BUCKET).file(`${gcsEvent.id}/${gcsEvent.id}.config.json`).save(JSON.stringify(config));
        console.debug(`Running Speech transcription for ${JSON.stringify(fileReferences)}`);
        fileReferences.forEach((file)=>{
            let thisFile = file;
            if(thisFile.bucket != null && thisFile.name != null){
                let speechClient = clientSPEECH(config.TRANSCRIPT_CONFIG);
                transcriptPromises.push(new Promise(async (res, rej)=>{
                    try{
                        let transcriptResult = await _transcribeRecording(speechClient, config.TRANSCRIPT_CONFIG, thisFile);
                        console.debug(`Requested transcript for ${JSON.stringify({...thisFile, error: null})}`)
                        res();
                    }
                    catch(err){
                        console.debug(`Requested transcript for ${JSON.stringify({...thisFile, error:err.message})}`)
                        transcriptErrors.push({...thisFile, error: err.message});
                        res();
                    }
                }));
            } else {
                transcriptPromises.push(new Promise(async (res, rej)=>{
                    console.debug(`No recording to transcibe for ${JSON.stringify({...thisFile})}`);
                    res();
                }));
            }
        });
        await Promise.all(transcriptPromises).then( async ()=>{
            rtn = {};
            if(transcriptErrors!=[]){
                rtn = {
                    errors: transcriptErrors
                }; 
                await gClient.bucket(process.env.SECURE_BUCKET).file(`${gcsEvent.id}/${gcsEvent.id}.errors.json`).save(JSON.stringify(rtn));
            }
            console.debug("PROCESS COMPLETE");
        });
    });
    return rtn;
}

//Run Insights Job
let _exportToBigQuery = async (bClient, iClient, conversationName, calldata, config) => {
    if(config.BQEnabled){
        let conversation = await new Promise((res, rej)=>{
            iClient.getConversation(conversationName).then((result)=>{
                res(result);
            })
        }) 
        //console.log(conversation);
        let row = [
            {
                callid: calldata.callid,
                jobId: calldata.jobid,
                created: new Date().toISOString(),
                callData: JSON.stringify(calldata),
                conversation: JSON.stringify(conversation[0])
            }
        ];
        try{
            let response= await bClient
                .dataset(process.env.BQ_DATASET)
                .table(process.env.BQ_TABLE)
                .insert(row);
            console.log(`Conversation ${conversationName.name} for ${calldata.callid} added to BigQuery table ${process.env.BQ_TABLE}`);
            return response;
        }catch(err){
            console.log(`FAILED to add to BigQuery`);
            console.log(err);
            return {};
        }
    } else {
        return {};
    }

}

let _createInsightsConversation = async (iClient, transcriptRef, config) => {
    if(config.CCAIInsightsEnabled){
        const conversationRequest = {
            parent: `projects/${process.env.PROJECT_ID}/locations/europe-west2`,
            conversation: {
                dataSource: {
                    gcsSource: {
                        transcriptUri: transcriptRef.transcriptPath,
                        audioUri: transcriptRef.audioPath,
                    },
                },
                agentId: transcriptRef.call.callTo.indexOf("@")!=-1 ? transcriptRef.call.callTo : "Customer",
                medium: 'PHONE_CALL',
                callMetaData:{
                    agentChannel: 2,
                    customerChannel: 1
                },
                labels:{
                    ...transcriptRef.call,
                    "jobId": transcriptRef.jobid
                }
            }
        }
        try{
            let conversation = await iClient.createConversation(conversationRequest);
            console.log(`Conversation ${conversation[0].name} created for ${transcriptRef.callid}`);
            if(config.CCAIAnalysisEnabled){
                let analysis = await iClient.createAnalysis({parent: conversation[0].name});
                console.log(`Requested conversation analysis for  ${conversation[0].name}`);
            }
            return {name: conversation[0].name};
        } catch(err){
            console.log("ERROR!");
            console.log(err);
            return {};
        }
    } else {
        return {};
    }


}

let runInsightsProcess = async (gcsEvent) => {
    let gClient = clientGCS();
    let configFile = JSON.parse(await gClient.bucket(gcsEvent.bucket).file(`${gcsEvent.id}/${gcsEvent.id}.config.json`).download());
    console.debug(`Runing new Insights Job ${JSON.stringify(gcsEvent)}`);
    let resultsFile = JSON.parse(await gClient.bucket(gcsEvent.bucket).file(`${gcsEvent.id}/${gcsEvent.id}.results.json`).download());
    let iClient = clientINSIGHTS();
    let bClient = clientBIGQUERY();
    let thisTranscript = resultsFile.find(r=>gcsEvent.name.indexOf(r.callid)!=-1);
    thisTranscript.transcriptPath = `gs://${gcsEvent.bucket}/${gcsEvent.name}`;
    console.debug(`Creating Conversations Insights for ${JSON.stringify(thisTranscript)}`);
    let insightsResult = await _createInsightsConversation(iClient, thisTranscript, configFile.INSIGHTS);
    let bigQueryResult = await _exportToBigQuery(bClient, iClient, insightsResult,  thisTranscript, configFile.INSIGHTS)
    return { insights: insightsResult, bigQuery: bigQueryResult };
}


//Run Validation Job
let _validateTranscription = async (gClient, gcsEvent) => {
    let resultsFile = JSON.parse(await gClient.bucket(gcsEvent.bucket).file(`${gcsEvent.id}/${gcsEvent.id}.results.json`).download());
    let thisTranscript = resultsFile.find(r=>gcsEvent.name.indexOf(r.callid)!=-1);
    console.log(JSON.stringify(resultsFile));
    console.log(JSON.stringify(thisTranscript));
    let transcriptFile = JSON.parse(await gClient.bucket(process.env.SECURE_BUCKET).file(gcsEvent.name).download());
    let modifiedResults = [];
    let truthFileResults = ["channel,timeOffset,hypothesis,reference"];
    transcriptFile.results.forEach((row)=>{
        let newRow = JSON.parse(JSON.stringify(row));
        if(newRow.alternatives!=undefined){
            if(newRow.alternatives[0].words!=undefined){
                if(newRow.resultEndTime==undefined){
                    newRow.resultEndTime = row.alternatives[0].words[row.alternatives[0].words.length-1].endTime;
                }
            }
            if(thisTranscript.call.callDirection!="inbound"){
                newRow.channelTag = (newRow.channelTag==1 ? 2 : 1);
            }
            if(newRow.alternatives[0].transcript!=undefined){
                truthFileResults.push(`${newRow.channelTag},${newRow.alternatives[0].words[0].startTime},"${newRow.alternatives[0].transcript}",`)
                modifiedResults.push(newRow);
            }
            
        }
    })
    modifiedResults.sort((a,b)=>{
        if(parseFloat(a.alternatives[0].words[0].startTime.replace("s","")) < parseFloat(b.alternatives[0].words[0].startTime.replace("s",""))){
            return -1;
        } 
        if(parseFloat(b.alternatives[0].words[0].startTime.replace("s","")) < parseFloat(a.alternatives[0].words[0].startTime.replace("s",""))){
            return 1;
        } 
        return 0;
    });
    let saveResponse = await gClient.bucket(process.env.SECURE_BUCKET).file(gcsEvent.name.replace(".json",".validated.json")).save(JSON.stringify({"results":modifiedResults}));
    let configFile = JSON.parse(await gClient.bucket(gcsEvent.bucket).file(`${gcsEvent.id}/${gcsEvent.id}.config.json`).download());
    console.log(`Transcript file validated`);
    if(configFile.INSIGHTS.TruthFileEnabled){
        let hypothesisResponse = await gClient.bucket(process.env.TRUTHS_BUCKET).file(gcsEvent.name.replace(".json",".hypothesis.csv")).save(truthFileResults.join('\r\n'));
    }
    console.log(`Truth File created`);
    return;
}

let runValidationProcess = async (gcsEvent) => {
    let gClient = clientGCS();
    console.debug(`Runing new Validation Job ${JSON.stringify(gcsEvent)}`);
    let validateResult = await _validateTranscription(gClient, gcsEvent);
    console.debug(`Validated ${gcsEvent.name}`);
    return { transcriptValidation: validateResult };
}

//helpers
let _getFileType = (gcsEvent) => {
    return (
        gcsEvent.name.indexOf("results.json")!=-1 ? "RESULT" : (
            gcsEvent.name.indexOf("config.json")!=-1 ? "CONFIG" : (
                gcsEvent.name.indexOf("errors")!=-1 ? "ERRORS" : (
                    gcsEvent.name.indexOf("hypothesis.csv")!=-1 ? "HYPOTHESIS" : (
                        gcsEvent.name.indexOf(".validated.")!=-1 ? "VALIDATEDTRANSCRIPT" : "TRANSCRIPT"
                    )
                )
            )
        )
    ); 
}


exports.processNewFile = async (event, context) => {
    console.debug("START");
    const gcsEvent = event.data
    ? JSON.parse(Buffer.from(event.data, 'base64').toString())
    : {};

    if(gcsEvent.bucket.toString().indexOf(process.env.CONFIG_BUCKET)!=-1){
        gcsEvent.id = gcsEvent.name.split('.')[0];
        let thisResult = await runTransciptProcess(gcsEvent);
    }
    

    if(gcsEvent.bucket.toString().indexOf(process.env.SECURE_BUCKET)!=-1){
        let fileType = _getFileType(gcsEvent);
        switch(fileType){
            case "TRANSCRIPT":
                gcsEvent.id = gcsEvent.name.split('/')[0];
                let tResult = await runValidationProcess(gcsEvent);
                console.debug(`Validation Job Result: ${JSON.stringify(tResult)}`);
                break;
            case "VALIDATEDTRANSCRIPT":
                gcsEvent.id = gcsEvent.name.split('/')[0];
                let vtResult = await runInsightsProcess(gcsEvent);
                console.debug(`Insights Job Results: ${JSON.stringify(vtResult)}`);
                break;
            default:
                console.debug(`No processing function found for ${gcsEvent.name}`);
                break;
        }
    }
    console.debug("END");

};