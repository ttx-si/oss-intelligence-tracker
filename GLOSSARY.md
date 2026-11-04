# Open Source Intelligence Tracker

追踪 GitHub 开源项目趋势信号、识别高动量爆发项目并向开发者提供结构化情报推送与检索的领域上下文。

## Language

**Repository**:
被追踪的 GitHub 开源项目实体，以平台分配的不可变整数标识作为唯一身份锚点，而非可随重命名或迁移改变的仓库路径。
_Avoid_: Project, Package, Repo string

**Snapshot**:
某个 **Repository** 在特定观测时刻的指标切面（如 Star 总量、增量及在榜位置），用于跨时间窗口计算增长动量。
_Avoid_: Log, Record, History row

**Signal Source**:
产出候选 **Repository** 列表或趋势指标的外部情报渠道（如榜单抓取、统计接口、人工精选期刊）。
_Avoid_: Plugin, Crawler, Spider

**Lifecycle State**:
刻画 **Repository** 当前所处增长动量阶段的互斥状态机，仅包含初次发现（`DISCOVERY`）、动量爆发（`SURGING`）、持续稳健（`SUSTAINED`）与热度冷却（`COOLED`）四个阶段。
_Avoid_: Status, Tag, Category

**Curation**:
独立于 **Lifecycle State** 之外的人工精选背书属性（如期刊收录期数与分类），与项目当前的实时增长动量正交共存。
_Avoid_: Curated state, Featured status

**Cooldown Window**:
同一 **Repository** 在触发某类高优状态推送后，抑制相同状态再次触发通知的静默防抖时间窗口（默认 72 小时），用于消除持续霸榜或阈值临界震荡带来的重复打扰。
_Avoid_: Rate limit, Sleep timer, Mute flag

**Batch Digest**:
单次调度周期内将所有满足跃迁条件的高动量 **Repository** 与新增 **Curation** 聚合而成的单条结构化通知载荷，避免逐条轰炸推送渠道。
_Avoid_: Single alert, Raw message
