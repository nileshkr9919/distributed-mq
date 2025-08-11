import { TopicOptions, TopicError, ValidationError } from './types';

export class Topic {
  private readonly _name: string;
  private readonly _partitions: Set<number>;
  private _nextPartitionId: number;

  constructor(options: TopicOptions) {
    if (!options.name || options.name.trim().length === 0) {
      throw new ValidationError('Topic name cannot be empty');
    }

    this._name = options.name;
    this._partitions = new Set<number>();
    this._nextPartitionId = 0;

    // Create initial partition(s)
    const numPartitions = options.partitions || 1;
    for (let i = 0; i < numPartitions; i++) {
      this.createPartition();
    }
  }

  get name(): string {
    return this._name;
  }

  createPartition(): void {
    const partitionId = this._nextPartitionId++;
    this._partitions.add(partitionId);
  }

  getPartitions(): number[] {
    return Array.from(this._partitions).sort((a, b) => a - b);
  }

  validatePartition(partition: number): boolean {
    if (!Number.isInteger(partition)) {
      throw new ValidationError('Partition must be an integer');
    }
    
    if (!this._partitions.has(partition)) {
      throw new TopicError(`Partition ${partition} does not exist in topic ${this._name}`);
    }

    return true;
  }

  toString(): string {
    return `Topic(name=${this._name}, partitions=${this.getPartitions().join(',')})`;
  }
}