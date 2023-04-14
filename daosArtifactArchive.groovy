/*
 * Based on cleanup/artifactCleanup from
 * https://github.com/jfrog/artifactory-user-plugins/
 * Copyright (C) 2014 JFrog Ltd.
 *
 * Copyright 2023 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// groovylint-disable DuplicateNumberLiteral, DuplicateStringLiteral
// groovylint-disable UnnecessaryGetter

// groovylint-disable JavaIoPackageAccess, LineLength
import org.apache.commons.lang3.StringUtils
import org.artifactory.api.repo.exception.ItemNotFoundRuntimeException
import org.artifactory.exception.CancelException
import org.artifactory.repo.RepoPathFactory
import org.artifactory.repo.RepoPath

import groovy.json.JsonSlurper
import groovy.time.TimeCategory
import groovy.time.TimeDuration
import groovy.transform.Field

import java.text.SimpleDateFormat

// groovylint-disable-next-line CompileStatic
@Field final String CONFIG_FILE_PATH = "plugins/${this.class.name}.json"
@Field final String DEFAULT_TIME_UNIT = 'month'
@Field final int DEFAULT_TIME_INTERVAL = 1

// groovylint-disable-next-line CompileStatic
class Global {

    static Boolean stopCleaning = false
    static Boolean pauseCleaning = false
    static int paceTimeMS = 0

}

// curl command example for running this plugin (Prior to Artifactory 5.x, use pipe '|' and not semi-colons ';' for parameters separation).
// curl -i -uadmin:password -X POST "http://localhost:8081/artifactory/api/plugins/execute/daosArtifactArchive?params=timeUnit=day;timeInterval=1;repos=libs-release-local;dryRun=true;paceTimeMS=2000;disablePropertiesSupport=true"
//
// For a HA cluster, the following commands have to be directed at the instance running the script. Therefore it is best to invoke
// the script directly on an instance so the below commands can operate on same instance
// curl -i -uadmin:password -X POST "http://localhost:8081/artifactory/api/plugins/execute/daosArtifactArchiveCtl?params=command=pause"
// curl -i -uadmin:password -X POST "http://localhost:8081/artifactory/api/plugins/execute/daosArtifactArchiveCtl?params=command=resume"
// curl -i -uadmin:password -X POST "http://localhost:8081/artifactory/api/plugins/execute/daosArtifactArchiveCtl?params=command=stop"
// curl -i -uadmin:password -X POST "http://localhost:8081/artifactory/api/plugins/execute/daosArtifactArchiveCtl?params=command=adjustPaceTimeMS;value=-1000"

String pluginGroup = 'cleaners'

executions {
    daosArtifactArchive(groups: [pluginGroup]) { params ->
        String timeUnit = params['timeUnit'] ? params['timeUnit'][0] as String : DEFAULT_TIME_UNIT
        int timeInterval = params['timeInterval'] ? params['timeInterval'][0] as int : DEFAULT_TIME_INTERVAL
        String[] repos = params['repos'] as String[]
        // groovylint-disable-next-line UnnecessaryBooleanInstantiation
        Boolean dryRun = params['dryRun'] ? new Boolean(params['dryRun'][0]) : false
        // groovylint-disable-next-line UnnecessaryBooleanInstantiation
        Boolean disablePropertiesSupport = params['disablePropertiesSupport'] ? new Boolean(params['disablePropertiesSupport'][0]) : false
        int paceTimeMS = params['paceTimeMS'] ? params['paceTimeMS'][0] as int : 0

        // Enable fallback support for deprecated month parameter
        if (params['months'] && !params['timeInterval']) {
            log.info('Deprecated month parameter is still in use, please use the new timeInterval parameter instead!', properties)
            timeInterval = params['months'][0] as int
        } else if (params['months']) {
            log.warn('Deprecated month parameter and the new timeInterval are used in parallel: month has been ignored.', properties)
        }

        cancelOnNullRepos(repos, 'repos parameter must be specified.')

        artifactCleanup(timeUnit, timeInterval, repos, log, paceTimeMS, dryRun, disablePropertiesSupport)
    }

    daosArtifactArchiveCtl(groups: [pluginGroup]) { params ->
        String command = params['command'] ? params['command'][0] as String : ''

        switch (command) {
            case 'stop':
                Global.stopCleaning = true
                log.info 'Stop request detected'
                break
            case 'adjustPaceTimeMS':
                int adjustPaceTimeMS = params['value'] ? params['value'][0] as int : 0
                int newPaceTimeMS = ((Global.paceTimeMS + adjustPaceTimeMS) > 0) ? (Global.paceTimeMS + adjustPaceTimeMS) : 0
                log.info "Pacing adjustment request detected, adjusting old pace time ($Global.paceTimeMS) by $adjustPaceTimeMS to new value of $newPaceTimeMS"
                Global.paceTimeMS = newPaceTimeMS
                break
            case 'pause':
                Global.pauseCleaning = true
                log.info 'Pause request detected'
                break
            case 'resume':
                Global.pauseCleaning = false
                log.info 'Resume request detected'
                break
            default:
                log.info "Missing or invalid command, '$command'"
        }
    }
}

File configFile = new File(ctx.artifactoryHome.etcDir, CONFIG_FILE_PATH)

if (configFile.exists()) {
    // groovylint-disable-next-line NoDef, VariableTypeRequired
    def config = new JsonSlurper().parse(configFile.toURL())
    log.info "Schedule job policy list: ${config.policies}"

    int count = 1
    config.policies.each { policySettings ->
        // groovylint-disable-next-line DuplicateListLiteral
        String cron = policySettings.containsKey('cron') ? policySettings.cron as String : ['0 23 * * * ?']
        String[] repos = policySettings.containsKey('repos') ? policySettings.repos as String[] : null
        String timeUnit = policySettings.containsKey('timeUnit') ? policySettings.timeUnit as String : DEFAULT_TIME_UNIT
        int timeInterval = policySettings.containsKey('timeInterval') ? policySettings.timeInterval as int : DEFAULT_TIME_INTERVAL
        int paceTimeMS = policySettings.containsKey('paceTimeMS') ? policySettings.paceTimeMS as int : 0
        // groovylint-disable-next-line UnnecessaryBooleanInstantiation
        Boolean dryRun = policySettings.containsKey('dryRun') ? new Boolean(policySettings.dryRun) : false
        // groovylint-disable-next-line UnnecessaryBooleanInstantiation
        Boolean disablePropertiesSupport = policySettings.containsKey('disablePropertiesSupport') ? new Boolean(policySettings.disablePropertiesSupport) : false

        cancelOnNullRepos(repos, 'repos parameter must be specified for cron jobs to be scheduled.')

        jobs {
            "daosArtifactArchive_$count"(cron: cron) {
                log.info "Policy settings for scheduled run at($cron): repo list($repos), timeUnit($timeUnit), timeInterval($timeInterval), paceTimeMS($paceTimeMS) dryrun($dryRun) disablePropertiesSupport($disablePropertiesSupport)"
                artifactCleanup(timeUnit, timeInterval, repos, log, paceTimeMS, dryRun, disablePropertiesSupport)
            }
        }
        count++
    }
}

// groovylint-disable-next-line ParameterCount, NoDef, MethodParameterTypeRequired
private boolean artifactCleanup(String timeUnit, int timeInterval, String[] repos, log, int paceTimeMS, Boolean dryRun = false, Boolean disablePropertiesSupport = false) {
    log.info "Starting artifact cleanup for repositories $repos, until $timeInterval ${timeUnit}s ago with pacing interval $paceTimeMS ms, dryrun: $dryRun, disablePropertiesSupport: $disablePropertiesSupport"

    cancelOnNullRepos(repos, 'repos parameter must be specified before initiating cleanup.')

    // Create Map(repo, paths) of skiped paths (or others properties supported in future ...)
    Map skip = [:]
    if (!disablePropertiesSupport && repos) {
        skip = getSkippedPaths(repos)
    }

    Calendar calendarUntil = Calendar.getInstance()

    calendarUntil.add(mapTimeUnitToCalendar(timeUnit), -timeInterval)

    // groovylint-disable-next-line SimpleDateFormatMissingLocale, UnnecessaryGetter
    String calendarUntilFormatted = new SimpleDateFormat('yyyy/MM/dd HH:mm').format(calendarUntil.getTime())
    log.info "Archiving all artifacts not downloaded since $calendarUntilFormatted"

    Global.stopCleaning = false
    int cntFoundArtifacts = 0
    int cntNoDeletePermissions = 0
    long bytesFound = 0
    long bytesFoundWithNoDeletePermission = 0

    Set artifactsCleanedUp = searches.artifactsNotDownloadedSince(
        calendarUntil, calendarUntil, repos)
    artifactsCleanedUp.find { item ->
        String repoKey = item.getRepoKey()
        log.info("    item = ${item.getClass()} ${item} repoKey = ${repoKey}")
        try {
            while ( Global.pauseCleaning ) {
                log.info 'Pausing by request'
                sleep( 60000 )
            }
            if ( Global.stopCleaning ) {
                log.info 'Stopping by request, ending loop'
                return true
            }
            if (!disablePropertiesSupport && skip[ item.repoKey ] && StringUtils.startsWithAny(item.path, skip[ item.repoKey ])) {
                if (log.isDebugEnabled()) {
                    log.debug "Skip $item"
                }
                return false
            }
            bytesFound += repositories.getItemInfo(item)?.getSize()
            cntFoundArtifacts++
            if (!security.canDelete(item)) {
                bytesFoundWithNoDeletePermission += repositories.getItemInfo(item)?.getSize()
                cntNoDeletePermissions++
            }
            String repoPath = item.getPath()
            String destKey = repoKey.replace('-stable-', '-archive-')
            RepoPath destItem = RepoPathFactory.create(destKey, repoPath)
            if (dryRun) {
                log.info "Found $item "
                log.info "\t==> currentUser: ${security.currentUser().getUsername()}"
                log.info "\t==> canDelete: ${security.canDelete(item)}"
                log.info "\t==> Dry Run for Moving ${item} to ${destItem}"
                if (repos.contains(repoKey)) {
                    log.info 'Stopping for this repo for debug, ending loop'
                    return true
                }
            } else {
                if (security.canDelete(item)) {
                    repositories.move(item, destItem)
                    log.info "Moved ${item} to ${destItem}"
                } else {
                    log.info "Can't delete $item (user ${security.currentUser().getUsername()} has no delete permissions), "
                }
            }
        } catch (ItemNotFoundRuntimeException ex) {
            log.info "Failed to find $item, skipping"
        }
        int sleepTime = (Global.paceTimeMS > 0) ? Global.paceTimeMS : paceTimeMS
        if (sleepTime > 0) {
            sleep( sleepTime )
        }
        return false
    }

    if (dryRun) {
        log.info "Dry run - nothing deleted. Found $cntFoundArtifacts artifacts consuming $bytesFound bytes"
        if (cntNoDeletePermissions > 0) {
            log.info "$cntNoDeletePermissions artifacts cannot be deleted due to lack of permissions ($bytesFoundWithNoDeletePermission bytes)"
        }
    } else {
        log.info "Finished cleanup, deleting $cntFoundArtifacts artifacts that took up $bytesFound bytes"
        if (cntNoDeletePermissions > 0) {
            log.info "$cntNoDeletePermissions artifacts could not be deleted due to lack of permissions ($bytesFoundWithNoDeletePermission bytes)"
        }
    }
}

private Map getSkippedPaths(String[] repos) {
    // groovylint-disable-next-line NoJavaUtilDate
    Date timeStart = new Date()
    Map skip = [:]
    for (String repoKey : repos) {
        List pathsTmp = []
        String aql = 'items.find({"repo":"' + repoKey + '","type": "any","@cleanup.skip":"true"}).include("repo", "path", "name", "type")'
        searches.aql(aql.toString()) { itemList ->
            // groovylint-disable-next-line NestedForLoop
            for (item in itemList) {
                String path = item.path + '/' + item.name
                // Root path case behavior
                if (item.path == '.') {
                    path = item.name
                }
                if (item.type == 'folder') {
                    path += '/'
                }
                if (log.isTraceEnabled()) {
                    log.trace 'skip found for ' + repoKey + ':' + path
                }
                pathsTmp.add(path)
            }
        }

        // Simplify list to have only parent paths
        List paths = []
        // groovylint-disable-next-line NestedForLoop
        for (path in pathsTmp.sort { itemList }) {
            if (paths.size == 0 || !path.startsWith(paths[-1])) {
                if (log.isTraceEnabled()) {
                    log.trace 'skip added for ' + repoKey + ':' + path
                }
                paths.add(path)
            }
        }

        if (paths.size > 0) {
            skip[repoKey] = paths.toArray(new String[paths.size])
        }
    }
    // groovylint-disable-next-line NoJavaUtilDate
    Date timeStop = new Date()
    TimeDuration duration = TimeCategory.minus(timeStop, timeStart)
    log.info 'Elapsed time to retrieve paths to skip: ' + duration
    return skip
}

// groovylint-disable-next-line MethodReturnTypeRequired, NoDef
private mapTimeUnitToCalendar(String timeUnit) {
    switch ( timeUnit ) {
        case 'minute':
            return Calendar.MINUTE
        case 'hour':
            return Calendar.HOUR
        case 'day':
            return Calendar.DAY_OF_YEAR
        case 'month':
            return Calendar.MONTH
        case 'year':
            return Calendar.YEAR
        default:
            String errorMessage = "$timeUnit is no valid time unit. Please check your request or scheduled policy."
            log.error errorMessage
            throw new CancelException(errorMessage, 400)
    }
}

private void cancelOnNullRepos(String[] repos, String errorMessage) {
    if ( !repos || (repos.length == 0) ) {
        log.error errorMessage
        throw new CancelException(errorMessage, 400)
    }
}
