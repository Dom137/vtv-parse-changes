const AWS = require('aws-sdk');
const ProxyAgent = require('proxy-agent').ProxyAgent;
const axios = require('axios');
const fs = require('fs');

let s3 = null;

const FILE_NAME = 'changes.data';

const BUCKET_NAME = process.env.AWS_BUCKET_NAME;
const SEPERATOR = process.env.APP_SEPERATOR;
const API_GW_FE_INDICATOR = process.env.APP_APIGW_INDICATOR;
const STBS_INDICATOR = process.env.APP_STBS_INDICATOR;
const CHG_DATA_IMPL_START_ATTR = process.env.APP_CHG_DATA_IMPL_START_ATTR;
const CHG_DATA_AFF_SRVS_ATTR = process.env.APP_CHG_DATA_AFF_SRVS_ATTR;
const CHG_DATA_AFF_OPCO_ATTR = process.env.APP_CHG_DATA_AFF_OPCO_ATTR;
const CHG_DATA_TITLE_ATTR = process.env.APP_CHG_DATA_TITLE_ATTR;
const CHG_DATA_STATUS_ATTR = process.env.APP_CHG_DATA_STATUS_ATTR;

// Configure which attributes to extract from the change
// use "const configAttributes = ['*']" to use all the attributes
// Otherwise, use a list of attributers:
// const configAttributes = [
//     "Change Title", "Environment", "Category", "Change Status", "Impl. Start (UTC)", "Impl. End (UTC)", "Affected OpCo's", "Affected Services", "Affected Components", "Activity Description", "Change Impact"
// ];
const configAttributes = process.env.APP_CHG_PROPS_TO_COPY.split(',');
const changeTypesOfInterest = process.env.APP_CHG_TYPES_OF_INTEREST;

// AIOps entity types
const OPCO_ENT_TYPE = 'opco';
const CHANGE_ENT_TYPE = 'change';
const OPCO_TO_CHG_REL_TYPE = 'has';

const AIOPS_AUTH_EP = process.env.AIOPS_AUTH_EP;
const AIOPS_AUTH_EP_USER = process.env.AIOPS_AUTH_EP_USER;
const AIOPS_AUTH_EP_PW = process.env.AIOPS_AUTH_EP_PW;
const AIOPS_OBS_JOBNAME = process.env.AIOPS_OBS_JOBNAME;
const AIOPS_TOPO_EP = process.env.AIOPS_TOPO_EP;
const AIOPS_RESOURCES_EP = process.env.AIOPS_RESOURCES_EP;
const AIOPS_REFERENCES_EP = process.env.AIOPS_REFERENCES_EP;

// will be set during runtime based on env var
let USE_PROXY = false;
let PROXY_URL = '';
let AIOPS_AUTH_TOKEN = '';
let CREATE_FILE = false;
let DEV_MODE = false;

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

// function to get the Auth token
async function getAuthToken() {
    try {
        const response = await axios.post(
            AIOPS_AUTH_EP,
            {
                username: AIOPS_AUTH_EP_USER,
                api_key: AIOPS_AUTH_EP_PW
            },
            {
                headers: {
                    'Content-Type': 'application/json',
                },
                proxy: false
            }
        );

        // Extract the token from the response data
        const token = response.data.token;

        // Return the token
        return token;
    } catch (error) {
        console.error('Error:', error.response ? error.response.data : error.message);
        return null;
    }
}

// helper function to post data to AIOps or write to file
async function sendToTopoApiOrFile(endpoint, data) {
    if (CREATE_FILE === true) {
        // based on the endpoint we decide it the resulting element
        // will be a Vertex(V) or Edge (E)
        let jsonString = JSON.stringify(data);
        if (endpoint === AIOPS_RESOURCES_EP || endpoint.startsWith(AIOPS_TOPO_EP) ) {
            jsonString = `V:${jsonString}\n`;
            try {
                fs.appendFileSync(FILE_NAME, jsonString);
                return true;
            } catch (err) {
                console.error('Error appending to file:', err);
                return false;
            }            
        }
        else if (endpoint === AIOPS_REFERENCES_EP) {
            jsonString = `E:${jsonString}\n`;
            try {
                fs.appendFileSync(FILE_NAME, jsonString);
                return true;
            } catch (err) {
                console.error('Error appending to file:', err);
                return false;
            }   
        }
        else {
            console.error(`Encountered an unexpexted target endpoint <${endpoint}> to choose between vertex or edge!`);
            return false;
        }

    }
    else {
        const headers = {
            'accept': 'application/json',
            'X-TenantID': 'cfd95b7e-3bc7-4006-a4a8-a73a79c71255',
            'JobId': AIOPS_OBS_JOBNAME,
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + AIOPS_AUTH_TOKEN
        };
    
        try {
            const response = await axios.post(endpoint, data, { headers, proxy: false });
            console.log(`Successfully sent data to topology API!`, response.status);
            return true;
        } catch (error) {
            console.log(error);
            console.error(`Error sending data to topology API!`, error.response ? error.response.data : error.message);
            return false;
        }
    }    
}

// helper funciton to convert a string to boolean
async function envStringToBoolean(envVar) {
    return envVar === 'true' || envVar === '1';
}

// Fetch the list of files from the root of the bucket (ignoring 'archive')
async function getRootFile() {
    const params = {
        Bucket: BUCKET_NAME,
        Prefix: '' // Only root files
    };

    try {
        const data = await s3.listObjectsV2(params).promise();
        const files = data.Contents
            .filter(file => !file.Key.startsWith('archive/')) // Ignore 'archive' folder
            .map(file => file.Key);

        if (files.length !== 1) {
            throw new Error(`Expected 1 file in the root, found ${files.length}`);
        }

        return files[0]; // Return the only file in the root
    } catch (err) {
        console.error('Error listing files:', err);
    }
}

// helper function to get a commom seperator on some change attributes
async function replaceAllOccurrences(str, charToReplace, replacementChar) {
    const regex = new RegExp(charToReplace, 'g'); // 'g' flag ensures all occurrences are replaced
    return str.replace(regex, replacementChar);
}

// helper function to remove unwanted 
//  - seperators from the change object
//  - Revision tags from the affected services
async function beautifyChangeObject(change) {
    if (change[CHG_DATA_AFF_SRVS_ATTR]) {
        const val = change[CHG_DATA_AFF_SRVS_ATTR];
        change[CHG_DATA_AFF_SRVS_ATTR] = await replaceAllOccurrences(await replaceAllOccurrences(await replaceAllOccurrences(val, ';', SEPERATOR), '\n', SEPERATOR), '\t', '')
        change[CHG_DATA_AFF_SRVS_ATTR] = change[CHG_DATA_AFF_SRVS_ATTR].replace(/\s*\([^)]*\)/g, '');
    }
    return change;
}

// function to fetch the OPCO topology elements
async function fetchTopologyOpcoData() {
    const url = `${AIOPS_TOPO_EP}?_field=uniqueId&_field=name&_type=${OPCO_ENT_TYPE}&_include_global_resources=false&_include_count=false&_include_status=false&_include_status_severity=false&_include_metadata=false&_return_composites=false`;

    try {
        const response = await axios.get(url, {
            headers: {
                'accept': 'application/json',
                'X-TenantID': 'cfd95b7e-3bc7-4006-a4a8-a73a79c71255',
                'Authorization': 'Bearer ' + AIOPS_AUTH_TOKEN
            },
            proxy: false
        });

        if (response.data) {
            const opcoToUniqueIdMapping = {};
            response.data._items.forEach(item => {
                opcoToUniqueIdMapping[item.name] = item.uniqueId;
            });
            return opcoToUniqueIdMapping;
        }
        else {
            console.error('Error collecting OPCOs from AIOps topology!');
            return null;
        }

    } catch (error) {
        console.log(error);
        console.error('Error fetching topology data:', error.message);
        return null;
    }

}

// function to fetch the topology elements by name
async function fetchTopologyDataByName(name) {
    const url = `${AIOPS_TOPO_EP}?_field=uniqueId&_field=matchTokens&_field=entityTypes&_field=name&_field=opco&_filter=name%3D${name}&_include_global_resources=false&_include_count=false&_include_status=false&_include_status_severity=false&_include_metadata=false&_return_composites=false`;

    try {
        const response = await axios.get(url, {
            headers: {
                'accept': 'application/json',
                'X-TenantID': 'cfd95b7e-3bc7-4006-a4a8-a73a79c71255',
                'Authorization': 'Bearer ' + AIOPS_AUTH_TOKEN
            },
            proxy: false
        });

        if (response.data && response.data._items) {
            return response.data._items
        }
        else {
            console.error(`Error collecting element with name ${name} from AIOps topology!`);
            return null;
        }

    } catch (error) {
        console.log(error);
        console.error('Error fetching topology data:', error.message);
        return null;
    }

}

// helper function to generate a unique ID
async function generateUID(change) {
    let uniqueId = `CHG_${Math.floor(Date.now() / 1000)}`;
    const chgStartTime = change[CHG_DATA_IMPL_START_ATTR];
    if (chgStartTime) {
        uniqueId = `CHG_${Math.floor(new Date(chgStartTime) / 1000)}`;
    }

    const chgAffectedSrvs = change[CHG_DATA_AFF_SRVS_ATTR];
    if (chgAffectedSrvs) {
        uniqueId = `${uniqueId}_${chgAffectedSrvs.replace(/\|/g, '_')}`;
    }
    return uniqueId;
}

// helper function to create the AIOps topology object for a change
async function addAiopsTopoPropperties(change) {
    change.uniqueId = await generateUID(change);
    change.entityTypes = [CHANGE_ENT_TYPE];
    change.matchTokens = [change.uniqueId];

    return change;
}
// function to transform the given change data to AIOps elements
async function prepareAndSendChangeData(changeData, opcoTopoData) {
    for (const change of changeData) {
        await beautifyChangeObject(change);
        await addAiopsTopoPropperties(change);

        const changeTypesOfInterestList = changeTypesOfInterest.split(',');
        if (DEV_MODE) {
            console.log('Processing changes with the following status: ' + changeTypesOfInterestList);
        }

        const changeType = change[CHG_DATA_STATUS_ATTR];
        if (changeType && changeTypesOfInterestList.includes(changeType)) {
            let changeTitle = change[CHG_DATA_TITLE_ATTR];
            if (changeTitle) {
                changeTitle = await replaceAllOccurrences(changeTitle.trim(), '\t');
                console.log(`Working on change with title ${changeTitle}...`);
                change.name = changeTitle;
                change.tags = ['change'];

                const affectedOpcosList = change[CHG_DATA_AFF_OPCO_ATTR];
                console.info(`Change with title ${changeTitle} is affecting ${affectedOpcosList.length} OPCO(s)!`);
                if (DEV_MODE) {
                    console.log('DEV-MODE: Raw affected OPCOs:')
                    console.log('    ', affectedOpcosList);
                }
                let affectedOpcos = [];
                for (let opco of affectedOpcosList) {
                    if (opco === 'SP')
                        opco = 'ES';
                    const opcoTopoEleUniqueId = opcoTopoData[opco];
                    if (opcoTopoEleUniqueId) {
                        affectedOpcos.push(opcoTopoEleUniqueId);
                    }
                    else {
                        console.warn(`Could not find an Opco in AIOps topology for letters ${opco} on change with title ${changeTitle}!`);
                    }
                }
                if (DEV_MODE) {
                    console.log('DEV-MODE: UniqueIDs of affected OPCOs:')
                    console.log('    ', affectedOpcos);
                }

                const affectedServicesString = change[CHG_DATA_AFF_SRVS_ATTR];
                if (DEV_MODE) {
                    console.log('DEV-MODE: Raw affected services:')
                    console.log('    ', affectedServicesString);
                }

                let affectedServices = [];
                if (affectedServicesString && affectedServicesString.length > 0) {
                    const affectedServicesList = affectedServicesString.split(SEPERATOR);
                    affectedServices = affectedServicesList.map(item => item.trim());
                    const numAffectedServices = affectedServices.length;
                    console.info(`Change with title ${changeTitle} is affecting ${numAffectedServices} service(s)!`);

                    for (const affectedService of affectedServices) {
                        if (affectedService !== '')
                            change.tags.push(affectedService);
                    }
                    if (DEV_MODE) {
                        console.debug('The following object will be sent to AIOps:');
                        console.log(change);
                    }
                    if (await sendToTopoApiOrFile(AIOPS_RESOURCES_EP, change)) {
                        console.log(`Successfully sent data for change ${changeTitle}`);
                    }
                    else {
                        console.error(`Error sending data for change ${changeTitle}`);
                    }

                    // this is the special case for changes that are affecting all "APIGW FE" and STBs:
                    // in this case, there is only one "affected service", which is called 
                    // "APIGW FE" or "STBs"
                    if (numAffectedServices == 1 && (affectedServices[0] === API_GW_FE_INDICATOR || affectedServices[0] === STBS_INDICATOR)) {
                        console.info(`Change with title ${changeTitle} is affecting the ${affectedServices[0] === API_GW_FE_INDICATOR ? API_GW_FE_INDICATOR : STBS_INDICATOR}. It will be mapped to the corresponding OPCOs, which are ${affectedOpcos}`);

                        // link the change to the affected opcos
                        for (const affectedOpco of affectedOpcos) {
                            if (affectedOpco !== '') {
                                const chgToOpcoRelation = {
                                    _fromUniqueId: affectedOpco,
                                    _toUniqueId: change.uniqueId,
                                    _edgeType: OPCO_TO_CHG_REL_TYPE
                                }
                                if (await sendToTopoApiOrFile(AIOPS_REFERENCES_EP, chgToOpcoRelation)) {
                                    console.log(`Successfully created relation from OPCO ${affectedOpco} to change ${change.name}`);
                                }
                                else {
                                    console.error(`Error creating relation from OPCO ${affectedOpco} to change ${change.name}:`);
                                }
                            }
                        }
                    }
                    else {
                        // find the affecting service in topology
                        // find them by their name, than check for the opco
                        for (const affectedService of affectedServices) {
                            if (affectedService !== '') {
                                console.log(`Looking for affected service ${affectedService} related to change ${change.name}...`);
                                const topoElements = await fetchTopologyDataByName(affectedService);
                                if (topoElements && topoElements.length > 0) {
                                    if (DEV_MODE) {
                                        console.log('DEV-MODE: Found the following affected service topo element:')
                                        console.log('    ', topoElements);
                                    }
                                    for (const topoElement of topoElements) {
                                        const eleOpco = topoElement.opco;
                                        const eleUniqueId = topoElement.uniqueId;
                                        if (eleOpco && eleUniqueId) {
                                            if (affectedOpcos.includes(eleOpco)) {
                                                console.log(`Found affected service ${affectedService} related to change ${change.name}, and it is linked to Opco ${eleOpco}.`);
                                                // set the change status on the element
                                                let eleUpdate = {};
                                                eleUpdate.uniqueId = eleUniqueId;
                                                eleUpdate.change = change[CHG_DATA_STATUS_ATTR];
                                                eleUpdate.entityTypes= topoElement.entityTypes;
                                                eleUpdate.matchTokens = topoElement.matchTokens;

                                                if (await sendToTopoApiOrFile(AIOPS_TOPO_EP + '/' + topoElement._id, eleUpdate)) {
                                                    console.log(`Successfully updated change status for affected service ${affectedService} for change ${changeTitle}`);
                                                }
                                                else {
                                                    console.error(`Error updating change status for affected service ${affectedService} for change ${changeTitle}`);
                                                }

                                                // create the relation
                                                const chgToEleRelation = {
                                                    _fromUniqueId: eleUniqueId,
                                                    _toUniqueId: change.uniqueId,
                                                    _edgeType: OPCO_TO_CHG_REL_TYPE
                                                }
                                                if (await sendToTopoApiOrFile(AIOPS_REFERENCES_EP, chgToEleRelation)) {
                                                    console.log(`Successfully created relation from affected service ${affectedService} to change ${change.name}`);
                                                }
                                                else {
                                                    console.error(`Error creating relation from affected service ${affectedService} to change ${change.name}:`);
                                                }


                                            }
                                        }
                                        else {
                                            console.error(`Found affected service ${affectedService} related to change ${change.name}, but it doesn't have an opco set. Won't link the change.`);
                                        }
                                    }
                                }
                                else {
                                    console.error(`Could not find affected service ${affectedService} in topology!`);
                                }
                            }
                        }
                    }
                }
                else {
                    console.error(`Change with title ${changeTitle} doesn't have any affected services! It won't be processed.`);
                }
            }
            else {
                console.error("ERROR: given change doesn't have a name! Skipping it...");
            }
        }
        else {
            console.warn("WARNING: Given change's type is not of interest! Skipping it...");
        }
    }
}

// Fetch the JSON file from S3
async function fetchFileFromS3(fileKey) {
    console.log(`Working on file ${fileKey}...`)
    const params = {
        Bucket: BUCKET_NAME,
        Key: fileKey
    };

    try {
        const data = await s3.getObject(params).promise();
        const jsonData = JSON.parse(data.Body.toString('utf-8'));
        return jsonData;
    } catch (err) {
        console.error('Error fetching file from S3:', err);
    }
}

// Extract the configured attributes from each object
async function extractAttributes(data) {
    if (DEV_MODE === true) {
        console.log('DEV-MODE: Raw change data:')
        console.log(data);
    }
    if (configAttributes.length == 1 && configAttributes[0] === '*') {
        console.log('All change properties will be sent to AIOps, no properties will be filtered out');
        return data;
    }
    else {
        return data.map(item => {
            const extracted = {};
            configAttributes.forEach(attr => {
                console.log(item[attr]);
                extracted[attr] = item[attr].trim() || null;
            });
            return extracted;
        });
    }

}

// Main function
(async function main() {
    DEV_MODE = await envStringToBoolean(process.env.APP_DEV_MODE);
    try {
        console.log("Trying to get Bearer token from AIOps Auth endpoint...");
        AIOPS_AUTH_TOKEN = await getAuthToken();
        let retryCount = 0
        while (retryCount < 3 && AIOPS_AUTH_TOKEN == null) {
            console.warn("Warning: Could not get AIOps Auth token. Retrying...");
            retryCount++;
            await new Promise(resolve => setTimeout(resolve, 1000));
            AIOPS_AUTH_TOKEN = await getAuthToken();
        }
        if (AIOPS_AUTH_TOKEN == null) {
            console.error("ERROR getting AIOps Auth token, retry limit reached! Cannot continue.");
            process.exit(1);
        }
        else {
            console.log("Bearer token from AIOps Auth endpoint received.");

            CREATE_FILE = await envStringToBoolean(process.env.APP_CREATE_FILE);
            if (CREATE_FILE === true) {
                console.log('Will be creating an output file instead of sending data to AIOps API...');
                // remove an old file if it exists
                if (fs.existsSync(FILE_NAME)) {
                    try {
                        fs.unlinkSync(FILE_NAME);
                        console.log(`Old file ${FILE_NAME} was deleted successfully.`);
                    } catch (err) {
                        console.error(`Error deleting file ${FILE_NAME}:`, err.message);
                    }
                }
            }

            USE_PROXY = await envStringToBoolean(process.env.APP_USE_PROXY);
            if (USE_PROXY) {
                PROXY_URL = process.env.APP_PROXY_URL;
                const proxyAgent = await new ProxyAgent(PROXY_URL);
                AWS.config.update({
                    httpOptions: { agent: proxyAgent }
                });
                console.log(`Using proxy url <${PROXY_URL}> to access AWS S3 bucket...`);
            }
            else {
                console.log("NOT using proxy to access AWS S3 bucket.");
            }
            s3 = new AWS.S3();

            const rootFile = await getRootFile();

            if (rootFile) {
                const jsonData = await fetchFileFromS3(rootFile);
    
                if (jsonData) {
                    const extractedData = await extractAttributes(jsonData);
                    AIOPS_AUTH_TOKEN = await getAuthToken();
                    // collect OPCOs from AIOps
                    const opcoTopoData = await fetchTopologyOpcoData();
                    if (!opcoTopoData) {
                        console.error('No OCPO data found in AIOps! No changes will be send to AIOps!');
                    }
                    else {
                        await prepareAndSendChangeData(extractedData, opcoTopoData);
                    }
                }
            }
        }
        
    } catch (err) {
        console.error('Error:', err);
    }
})();