const axios = require("axios");
const _ = require("lodash");
const utc = require("dayjs/plugin/utc");
const dayjs = require("dayjs");

dayjs.extend(utc);

const hirarchy = {
    0: "national",
    1: "region",
    2: "district",
    3: "subcounty",
    4: "facility",
};

module.exports.api = axios.create({
    baseURL: "http://172.27.1.167:3001/",
});

module.exports.updateQuery = `update programstageinstance set executiondate = created where executiondate < '2021-03-10' or executiondate > CURRENT_DATE + interval '1 day';`;
module.exports.allColumnsQuery = `select o.uid as orgUnit,
	o.path,
	ps.uid as stage,
	ps.name as stagename,
	(
		select ot.uid
		from organisationunit ot
		where ot.organisationunitid = ev.organisationunitid
	) as regOrgUnit,
	(
		select ot.path
		from organisationunit ot
		where ot.organisationunitid = ev.organisationunitid
	) as regPath,
	concat(te.uid, ev.uid) as id,
	en.created as "pi_created",
	en.lastupdated as "pi_last_updated",
	en.occurreddate as "pi_incident_date",
	en.enrollmentdate as "pi_enrollment_date",
	en.completedby as "pi_completed_by",
	en.deleted as "pi_deleted",
	en.storedby as "pi_stored_by",
	en.status as "pi_status",
	ev.uid as "event_uid",
	ev.created as "event_created",
	ev.lastupdated as "event_last_updated",
	ev.deleted as "event_deleted",
	ev.storedby as "event_stored_by",
	ev.scheduleddate as "event_duedate",
	ev.occurreddate as "event_execution_date",
	ev.status as "event_status",
	ev.completedby as "event_completed_by",
	ev.completeddate as "event_completed_date",
	ev.createdbyuserinfo->>'username' as "event_created_by",
	ev.lastupdatedbyuserinfo->>'username' as "event_lastupdated_by",
	p.name as "program",
	p.uid as "program_uid",
	te.uid as "tei_uid",
	te.created as "tei_created",
	te.lastupdated as "tei_last_updated",
	te.inactive as "tei_inactive",
	te.deleted as "tei_deleted",
	te.storedby as "tei_stored_by",
	(
		select jsonb_object_agg(tea.uid, value) AS months
		from trackedentityattributevalue teav
			inner join trackedentityattribute tea using(trackedentityattributeid)
		where teav.trackedentityid = te.trackedentityid
	) as attributes,
	eventdatavalues
from event ev
	inner join programstage ps using(programstageid)
	inner join organisationunit o using(organisationunitid)
	inner join enrollment en using(enrollmentid)
	inner join program p on(p.programid = en.programid)
	inner join trackedentity te using(trackedentityid)`;

module.exports.makeQuery = (condition) => {
    return `${this.allColumnsQuery} 
    ${condition}`;
};
module.exports.queryByProgram = (program) =>
    this.makeQuery(` where p.uid = '${program}'`);

module.exports.intervalQuery = (minutes) => {
    const end = dayjs()
        .utc()
        .subtract(minutes, "minutes")
        .format("YYYY-MM-DD HH:mm:ss");
    return this.makeQuery(
        `where p.uid = 'yDuAzyqYABS' and (en.created >= '${end}' or en.lastupdated >= '${end}');`
    );
};
module.exports.intervalQuery2 = (minutes) => {
    const end = dayjs()
        .utc()
        .subtract(minutes, "minutes")
        .format("YYYY-MM-DD HH:mm:ss");
    return this.makeQuery(
        `where p.uid = 'yDuAzyqYABS' and (ev.created >= '${end}' or ev.lastupdated >= '${end}');`
    );
};

module.exports.monthlyBacklogQuery = (date) =>
    this.makeQuery(`where p.uid = 'yDuAzyqYABS'
  and (
    to_char(tei.created, 'YYYY-MM') = '${date}'
    or to_char(en.lastupdated, 'YYYY-MM') = '${date}'
    or to_char(ev.created, 'YYYY-MM') = '${date}'
    or to_char(ev.lastupdated, 'YYYY-MM') = '${date}'
  );`);

module.exports.processAndInsert = async (index, rows) => {
    const all = rows.map(({ path, ...others }) => {
        const units = _.fromPairs(
            String(path)
                .split("/")
                .slice(1)
                .map((x, i) => {
                    return [hirarchy[i] || "other", x];
                })
        );
        return {
            ...others,
            path: units,
        };
    });
    try {
        const { data } = await this.api.post(`wal/bulk?index=${index}`, {
            data: all,
        });
        console.log(data.inserted);
        data.errorDocuments.forEach(({ error, document }) =>
            console.error(error, document)
        );
    } catch (error) {
        console.log(error.message);
    }
};

module.exports.createBacklogQuery = (start, end) =>
    this.makeQuery(
        `where p.uid = 'yDuAzyqYABS' and en.lastupdated >= '${start}' and en.lastupdated < '${end}';`
    );
module.exports.createBacklogQuery2 = (start, end) =>
    this.makeQuery(
        `where p.uid = 'yDuAzyqYABS' and ev.created >= '${start}' and ev.created < '${end}';`
    );

module.exports.queryByNIN = (nin) =>
    this.makeQuery(
        `where p.uid = 'yDuAzyqYABS' and en.trackedentityinstanceid = (select trackedentityinstanceid from trackedentityattributevalue where value = '${nin}');`
    );

module.exports.processAndInsert2 = async (index, rows) => {
    try {
        const all = rows.map(
            ({ attributes, eventdatavalues, path, regpath, ...rest }) => {
                const processedEvents = _.fromPairs(
                    Object.entries(eventdatavalues).flatMap(
                        ([dataElement, value]) => {
                            return [
                                [dataElement, value.value],
                                [`${dataElement}_created`, value.created],
                                [
                                    `${dataElement}_last_updated`,
                                    value.lastUpdated,
                                ],
                                [
                                    `${dataElement}_created_by`,
                                    value.createdByUserInfo?.username,
                                ],
                                [
                                    `${dataElement}_last_updated_by`,
                                    value.lastUpdatedByUserInfo?.username,
                                ],
                            ];
                        }
                    )
                );
                rest = {
                    ...rest,
                    ...attributes,
                    ...processedEvents,
                    same_user: false,
                };
                if (path) {
                    const eventOrgUnit = _.fromPairs(
                        String(path)
                            .split("/")
                            .slice(1)
                            .map((x, i) => {
                                return [`event_level${i + 1}`, x];
                            })
                    );

                    rest = { ...rest, ...eventOrgUnit };
                }
                if (regpath) {
                    const registrationOrgUnit = _.fromPairs(
                        String(regpath)
                            .split("/")
                            .slice(1)
                            .map((x, i) => {
                                return [`reg_level${i + 1}`, x];
                            })
                    );
                    rest = { ...rest, ...registrationOrgUnit };
                }
                if (
                    rest["LUIsbsm3okG"] &&
                    rest["bbnyNYD1wgS"] &&
                    rest["LUIsbsm3okG_created_by"] ===
                        rest["bbnyNYD1wgS_created_by"] &&
                    rest["LUIsbsm3okG_created"].slice(0, 10) ===
                        rest["bbnyNYD1wgS_created"].slice(0, 10)
                ) {
                    rest = { ...rest, same_user: true };
                }
                return rest;
            }
        );
        const { data } = await this.api.post(`wal/bulk?index=${index}`, {
            data: all,
        });
        console.log(data.inserted);
        data.errorDocuments.forEach(({ error, document }) =>
            console.error(error, document)
        );
    } catch (error) {
        console.log(error.message);
    }
};

module.exports.batchSize = 2000;
