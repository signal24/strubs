import _ from 'lodash';

import { config } from '../config';
import { ioManager } from './manager';
import { Plan } from './plan';
import type { Volume } from './volume';

export class Planner {
    generatePlan(fileSize: number): Plan {
        const plan = new Plan();

        plan.fileSize = fileSize;

        // this will be heavily based on configuration later on in life

        // data & parity slice count
        plan.dataSliceCount = config.dataSliceCount;
        plan.paritySliceCount = config.paritySliceCount;

        // chunk size
        plan.chunkSize = config.chunkSize;

        // TODO: determine the slice size here so we can find available volumes
        // maybe???

        if (plan.dataSliceCount === null || plan.paritySliceCount === null)
            throw new Error('plan slice counts are not configured');

        const dataSliceCount = plan.dataSliceCount;
        const paritySliceCount = plan.paritySliceCount;

        // determine what volumes are available
        const availableVolumes = ioManager.getWritableVolumes();
        const volumeById = new Map<number, Volume>();
        availableVolumes.forEach(volume => volumeById.set(volume.id, volume));

        // make sure we have at least as many available volumes as total slices
        if (availableVolumes.length < plan.totalSliceCount)
            throw new Error('not enough volumes available for planned slice count');

        // order into most space free, one per group, looping through each group
        let sortedVolumes = this._getSortedVolumesByBytesFreeWithAlternatingGroups(availableVolumes);

        if (sortedVolumes.length < plan.totalSliceCount) {
            sortedVolumes = this._getSortedVolumesByBytesFree(availableVolumes);
        }

        if (sortedVolumes.length < plan.totalSliceCount)
            throw new Error('not enough volumes available for planned slice count');

        let targetVolumes = sortedVolumes.slice(0, dataSliceCount + paritySliceCount);
        targetVolumes = _.shuffle(targetVolumes);

        // get the target volumes as the first x sorted volumes, where x is the target volumes
        const dataVolumes = targetVolumes.slice(0, dataSliceCount);
        const parityVolumes = targetVolumes.slice(dataSliceCount, dataSliceCount + paritySliceCount);

        // set the volume IDs for the data slices and parity slices
        plan.dataVolumes = dataVolumes.map(volume => volume.id);
        plan.parityVolumes = parityVolumes.map(volume => volume.id);
        plan.computeSliceSize();

        // reserve space
        this._reserveSliceCapacity(plan, plan.dataVolumes, 'data', volumeById);
        this._reserveSliceCapacity(plan, plan.parityVolumes, 'parity', volumeById);

        return plan;
    }

    private _getSortedVolumesByBytesFreeWithAlternatingGroups(volumes: Volume[]): Volume[] {
        const volumesByGroup: Record<string, Volume[]> = {};

        volumes.forEach(volume => {
            const groupKey = String(volume.deviceGroup ?? 'ungrouped');
            if (!volumesByGroup[groupKey])
                volumesByGroup[groupKey] = [];
            volumesByGroup[groupKey].push(volume);
        });

        Object.keys(volumesByGroup).forEach(groupId => {
            const groupedVolumes = volumesByGroup[groupId];
            groupedVolumes.sort((a, b) => {
                const freeB = (b.bytesFree ?? 0) - b.bytesPending;
                const freeA = (a.bytesFree ?? 0) - a.bytesPending;
                return freeB - freeA;
            });
        });

        const sortedVolumes: Volume[] = [];
        while (sortedVolumes.length < volumes.length) {
            for (let groupId in volumesByGroup) {
                const groupedVolumes = volumesByGroup[groupId];
                if (!groupedVolumes.length) continue;
                const nextVolume = groupedVolumes.shift();
                if (nextVolume)
                    sortedVolumes.push(nextVolume);
            }
        }

        return sortedVolumes;
    }

    private _getSortedVolumesByBytesFree(volumes: Volume[]): Volume[] {
        const sortedVolumes = [ ...volumes ];
        sortedVolumes.sort((a, b) => {
            const freeB = (b.bytesFree ?? 0) - b.bytesPending;
            const freeA = (a.bytesFree ?? 0) - a.bytesPending;
            return freeB - freeA;
        });
        return sortedVolumes;
    }

    private _reserveSliceCapacity(plan: Plan, volumeIds: number[], sliceType: 'data' | 'parity', volumeById: Map<number, Volume>): void {
        const sliceSize = plan.sliceSize ?? 0;
        volumeIds.forEach(volumeId => {
            const volume = volumeById.get(volumeId);
            volume?.reserveSpace(sliceSize);
        });
    }
}

export const planner = new Planner();
